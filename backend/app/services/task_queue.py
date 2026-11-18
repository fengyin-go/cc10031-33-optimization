"""任务队列领域服务 — 消费执行计划生成幂等排队任务，支持中断恢复。

数据工程核心：数据标准化与字段映射转换 — 跨模块数据产物

数据流:
    channel("rpa.steps_submitted") ← step_editor（携带 plan_key / 版本快照 / 稳定步骤键）
    → task_queue 消费 → 按 (plan_key, step_key) 复合唯一生成排队任务
    → 调度执行 / 中断恢复只续做未完成任务

幂等与恢复约定:
    - 任务编号只由流程 ID 与步骤序号拼接（TASK-{bot}-{seq}），同一计划重复提交复用编号
    - 排队项以 (执行计划键, 稳定步骤键) 复合唯一，不同版本计划不会互相混淆
    - 每个任务固化流程版本快照，处理前核对快照一致性
    - 中断恢复按计划版本 + 步骤键补回缺失边界，已完成/已取消步骤不再入队
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import PlanRecord, store

# 终态：具备清晰终态的任务不会被中断恢复重新入队
TERMINAL_STATUSES = ("已完成", "已取消")
ACTIVE_STATUSES = ("排队中", "执行中")


class TaskQueueService(BaseModuleService):
    MODULE = "task_queue"
    ENTITY = "排队任务"
    OPERATION_TERM = "采集"
    LIST_FIELDS = [
        "任务编号", "流程名称", "执行计划", "流程版本", "步骤键",
        "提交时间", "优先级", "预计执行", "排队时长", "当前状态",
    ]
    REQUIRED_FIELDS = ["任务编号", "流程名称", "提交时间"]
    STATUS_ORDER = ["排队中", "执行中", "已完成", "已取消", "执行失败"]
    ACTION_RULES = {"取消任务": "已取消", "提升优先级": "排队中"}
    NEGATIVE_ACTIONS = []

    # 复合唯一键的串行化锁：并发两次提交只生成一份排队项
    _idempotency_lock = threading.RLock()

    # ---- 幂等消费 ----

    def consume_steps_to_queue(self, conflict_policy: str = "cancel_old") -> dict[str, Any]:
        """消费 step_editor 提交的执行计划，幂等生成任务队列条目。

        从 channel("rpa.steps_submitted") 读取计划，按执行计划清单中的稳定
        步骤边界逐条生成任务。同一 (plan_key, step_key) 已存在时复用条目，
        重复提交不会产生重复排队项。

        Args:
            conflict_policy: 同一流程多版本计划同时排队时的处理边界。
                - "cancel_old": 最新计划版本取消旧版本未执行项（默认）
                - "coexist": 两个计划各自完成，步骤键互不混淆

        Returns:
            生成/复用/取消统计与任务 ID 清单
        """
        msgs = store.channel.consume(
            consumer=self.MODULE,
            topic="rpa.steps_submitted",
        )
        if not msgs:
            return {
                "generated": 0, "reused": 0, "plans_consumed": 0,
                "canceled_by_conflict": 0, "task_ids": [],
            }

        generated: list[int] = []
        reused: list[int] = []
        canceled: list[int] = []
        with self._idempotency_lock:
            for msg in msgs:
                plan = self._resolve_plan(msg.payload)
                if plan is None:
                    # 无法解析稳定步骤边界的消息不产生脏排队项
                    store.channel.confirm(msg.msg_id, self.MODULE)
                    continue

                if conflict_policy == "cancel_old":
                    canceled.extend(self._cancel_superseded_plans(plan))

                for step in plan.manifest:
                    existing = self._find_task(plan.plan_key, step.step_key)
                    if existing is not None:
                        # 重复提交同一计划：复用编号与排队项
                        reused.append(existing["id"])
                        continue
                    entry = self._build_task_entry(plan, step)
                    store.rows(self.MODULE).append(entry)
                    generated.append(entry["id"])

                store.channel.confirm(msg.msg_id, self.MODULE)

        return {
            "generated": len(generated),
            "reused": len(reused),
            "plans_consumed": len(msgs),
            "canceled_by_conflict": len(canceled),
            "task_ids": generated + reused,
            "generated_ids": generated,
            "reused_ids": reused,
            "canceled_ids": canceled,
        }

    def resync_plans_to_queue(self, conflict_policy: str = "cancel_old") -> dict[str, Any]:
        """按执行计划注册表核对排队项：不依赖通道的幂等对账。

        重复提交或消息中断后都可调用 —— 以 (plan_key, step_key) 复合唯一
        为准，缺什么补什么，已存在的一律复用。
        """
        generated: list[int] = []
        reused: list[int] = []
        canceled: list[int] = []
        with self._idempotency_lock:
            for plan in store.execution_plans.list_plans():
                if plan.canceled:
                    # 已被取代的旧版本计划不再补排队项，只保留终态
                    continue
                if conflict_policy == "cancel_old":
                    canceled.extend(self._cancel_superseded_plans(plan))
                for step in plan.manifest:
                    existing = self._find_task(plan.plan_key, step.step_key)
                    if existing is not None:
                        reused.append(existing["id"])
                        continue
                    entry = self._build_task_entry(plan, step)
                    store.rows(self.MODULE).append(entry)
                    generated.append(entry["id"])
        return {
            "generated": len(generated),
            "reused": len(reused),
            "canceled_by_conflict": len(canceled),
            "generated_ids": generated,
            "reused_ids": reused,
            "canceled_ids": canceled,
        }

    def _resolve_plan(self, payload: dict[str, Any]) -> PlanRecord | None:
        """从通道负载解析执行计划；兼容旧格式消息，就地补齐计划注册表。"""
        bot_id = int(payload.get("bot_id", 0) or 0)
        if not bot_id:
            return None
        plan_key = payload.get("plan_key")
        if plan_key:
            plan = store.execution_plans.get_plan(plan_key)
            if plan is not None:
                return plan

        bot_version = str(payload.get("bot_version") or "1.0.0")
        raw_steps = payload.get("steps", [])
        if not raw_steps:
            return None
        steps = []
        for i, step in enumerate(raw_steps):
            seq = int(step.get("seq", i + 1))
            steps.append({
                "seq": seq,
                "step_id": step.get("step_id", 0),
                "name": step.get("name", f"Step-{seq}"),
                "type": step.get("type", ""),
                "step_key": step.get("step_key")
                or store.execution_plans.build_step_key(bot_id, seq),
            })
        snapshot = payload.get("snapshot") or {
            "bot_id": bot_id, "版本号": bot_version,
        }
        plan, _ = store.execution_plans.upsert_plan(
            bot_id=bot_id, bot_version=bot_version, steps=steps, snapshot=snapshot,
        )
        return plan

    def _cancel_superseded_plans(self, active_plan: PlanRecord) -> list[int]:
        """最新计划版本排队时，取消同一流程旧版本计划的未执行项。"""
        canceled_ids: list[int] = []
        for plan in store.execution_plans.list_plans(active_plan.bot_id):
            if plan.plan_key == active_plan.plan_key or plan.canceled:
                continue
            old_tasks = [
                t for t in self._plan_tasks(plan.plan_key)
                if t.get("status") in ACTIVE_STATUSES
            ]
            if not old_tasks:
                continue
            reason = f"被最新计划版本 {active_plan.bot_version} 取代，旧版本未执行项取消"
            for task in old_tasks:
                self._mark_terminal(task, "已取消", reason=reason)
                canceled_ids.append(task["id"])
            store.execution_plans.mark_canceled(plan.plan_key, reason)
        return canceled_ids

    def _build_task_entry(self, plan: PlanRecord, step) -> dict[str, Any]:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        task_number = f"TASK-{plan.bot_id}-{step.seq:03d}"
        rows = store.rows(self.MODULE)
        entry = {
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "任务编号": task_number,
            "流程名称": f"Bot-{plan.bot_id}",
            "执行计划": plan.plan_key,
            "流程版本": plan.bot_version,
            "步骤键": step.step_key,
            "提交时间": now,
            "优先级": "高" if step.seq <= 3 else "中",
            "预计执行": step.name,
            "排队时长": "0s",
            "当前状态": "排队中",
            # 领域扩展字段
            "status": "排队中",
            "pending": True,
            "abnormal": False,
            "plan_key": plan.plan_key,
            "bot_id": plan.bot_id,
            "bot_version": plan.bot_version,
            "step_key": step.step_key,
            "seq": step.seq,
            "step_id": step.step_id,
            "step_type": step.step_type,
            "version_snapshot": dict(plan.snapshot),
            "snapshot_verified": True,
            "retry_count": 0,
            "失败原因": "",
        }
        return entry

    # ---- 查询辅助 ----

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """沿用既有处理边界（取消任务/提升优先级），并同步处理详情与结果清单。"""
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        # 通用骨架只改 status，这里同步展示字段与终态标记
        entry["当前状态"] = entry["status"]
        if action == "取消任务":
            self._mark_terminal(entry, "已取消", reason="人工取消任务")
        return entry, message

    def _find_task(self, plan_key: str, step_key: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("plan_key") == plan_key and row.get("step_key") == step_key:
                return row
        return None

    def _plan_tasks(self, plan_key: str) -> list[dict[str, Any]]:
        return [
            r for r in store.rows(self.MODULE)
            if r.get("plan_key") == plan_key
        ]

    def plan_progress(self, plan_key: str) -> dict[str, Any]:
        """按计划版本 + 步骤键给出步骤完成边界。"""
        plan = store.execution_plans.get_plan(plan_key)
        if plan is None:
            return {"ok": False, "error": f"执行计划 {plan_key} 不存在"}
        tasks = {t.get("step_key"): t for t in self._plan_tasks(plan_key)}

        by_status = {s: 0 for s in self.STATUS_ORDER}
        completed_seqs: list[int] = []
        boundaries = []
        for step in plan.manifest:
            task = tasks.get(step.step_key)
            status = task.get("status") if task else "未入队"
            if task:
                by_status[task["status"]] = by_status.get(task["status"], 0) + 1
            if status == "已完成":
                completed_seqs.append(step.seq)
            boundaries.append({
                "step_key": step.step_key,
                "seq": step.seq,
                "任务编号": task.get("任务编号") if task else None,
                "status": status,
                "terminal": status in TERMINAL_STATUSES,
            })

        completed_seqs.sort()
        completion_boundary = 0
        for seq in completed_seqs:
            if seq == completion_boundary + 1:
                completion_boundary = seq
            else:
                break

        return {
            "ok": True,
            "plan_key": plan_key,
            "bot_id": plan.bot_id,
            "bot_version": plan.bot_version,
            "plan_canceled": plan.canceled,
            "total_steps": plan.total_steps,
            "by_status": by_status,
            "completed": by_status.get("已完成", 0),
            "canceled": by_status.get("已取消", 0),
            "failed": by_status.get("执行失败", 0),
            "remaining": by_status.get("排队中", 0) + by_status.get("执行中", 0),
            "completion_boundary_seq": completion_boundary,
            "boundaries": boundaries,
        }

    def list_plan_boundaries(self, bot_id: int | None = None) -> list[dict[str, Any]]:
        """任务队列视图：按流程版本和计划列出每个计划的步骤完成边界。"""
        plans = store.execution_plans.list_plans(bot_id)
        return [self.plan_progress(plan.plan_key) for plan in plans]

    # ---- 流程版本快照核对 ----

    def verify_task_snapshot(self, task_id: int) -> dict[str, Any]:
        """核对排队处理项对应的流程版本快照是否与当前计划快照一致。"""
        entry = store.find(self.MODULE, task_id)
        if entry is None:
            return {"ok": False, "error": f"任务 {task_id} 不存在"}
        plan = store.execution_plans.get_plan(entry.get("plan_key", ""))
        if plan is None:
            return {
                "ok": False, "task_id": task_id,
                "error": "执行计划已不存在，无法核对版本快照",
            }
        stored = entry.get("version_snapshot", {})
        mismatches = [
            key for key, value in plan.snapshot.items()
            if stored.get(key) != value
        ]
        entry["snapshot_verified"] = not mismatches
        return {
            "ok": not mismatches,
            "task_id": task_id,
            "plan_key": plan.plan_key,
            "stored_snapshot": stored,
            "current_snapshot": plan.snapshot,
            "mismatch_fields": mismatches,
        }

    # ---- 状态流转与失败恢复 ----

    def update_queue_status(self, task_id: int,
                             status: str, result: dict[str, Any] | None = None,
                             failure_reason: str = "") -> dict[str, Any] | None:
        """更新任务状态并在执行完成时写 execution_log 通道。"""
        entry = store.find(self.MODULE, task_id)
        if entry is None:
            return None

        valid_statuses = {"执行中", "已完成", "已取消", "执行失败"}
        if status not in valid_statuses:
            return None

        if status == "执行失败":
            self._mark_failed(entry, failure_reason or "执行异常未提供原因")
        elif status == "已取消":
            self._mark_terminal(entry, status, reason=failure_reason or "人工取消任务")
        else:
            entry["status"] = status
            entry["当前状态"] = status
            entry["pending"] = status not in TERMINAL_STATUSES
            entry["abnormal"] = False
            if status == "已完成":
                entry["终态时间"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        if result:
            entry["result"] = result

        # 完成后写入执行日志通道
        if status in ("已完成", "执行失败", "已取消"):
            exec_payload = {
                "task_id": task_id,
                "task_number": entry.get("任务编号"),
                "plan_key": entry.get("plan_key"),
                "step_key": entry.get("step_key"),
                "final_status": status,
                "processed_by": self.MODULE,
                "result": result or {},
            }
            store.channel.publish(
                topic="rpa.task_completed",
                source=self.MODULE,
                payload=exec_payload,
            )

        return {
            "task_id": task_id,
            "status": status,
            "task_number": entry.get("任务编号"),
            "plan_key": entry.get("plan_key"),
            "step_key": entry.get("step_key"),
        }

    def _mark_terminal(self, entry: dict[str, Any], status: str, reason: str = "") -> None:
        entry["status"] = status
        entry["当前状态"] = status
        entry["pending"] = False
        entry["abnormal"] = status == "执行失败"
        entry["终态时间"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        if reason:
            entry["终态说明"] = reason

    def _mark_failed(self, entry: dict[str, Any], reason: str) -> None:
        entry["status"] = "执行失败"
        entry["当前状态"] = "执行失败"
        entry["pending"] = False
        entry["abnormal"] = True
        entry["失败原因"] = reason
        entry["终态时间"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        # 登记平台级失败恢复记录，异常原因可追踪
        store.recovery.log(
            module=self.MODULE,
            batch_ref=entry.get("plan_key", ""),
            step=entry.get("step_key", ""),
            error_detail=reason,
        )

    def list_failed_tasks(self) -> list[dict[str, Any]]:
        """失败清单（空态返回空列表，表示暂无失败）。"""
        return [
            {
                "task_id": r.get("id"),
                "任务编号": r.get("任务编号"),
                "plan_key": r.get("plan_key"),
                "step_key": r.get("step_key"),
                "失败原因": r.get("失败原因", ""),
                "retry_count": r.get("retry_count", 0),
                "终态时间": r.get("终态时间", ""),
            }
            for r in store.rows(self.MODULE)
            if r.get("status") == "执行失败"
        ]

    def retry_failed_task(self, task_id: int) -> dict[str, Any]:
        """重试失败任务：中断异常必须说明原因，重试只针对失败项。"""
        entry = store.find(self.MODULE, task_id)
        if entry is None:
            return {"ok": False, "error": f"任务 {task_id} 不存在"}
        if entry.get("status") != "执行失败":
            return {
                "ok": False,
                "error": f"任务当前状态为「{entry.get('status')}」，仅执行失败的任务可重试",
            }
        plan = store.execution_plans.get_plan(entry.get("plan_key", ""))
        if plan is not None and plan.canceled:
            return {"ok": False, "error": "所属计划版本已被取消，不能重试，请改用最新计划版本"}
        if not entry.get("失败原因"):
            return {"ok": False, "error": "中断异常缺少失败原因，无法重试"}

        entry["status"] = "排队中"
        entry["当前状态"] = "排队中"
        entry["pending"] = True
        entry["abnormal"] = False
        entry["retry_count"] = int(entry.get("retry_count", 0)) + 1
        entry["上次失败原因"] = entry.pop("失败原因", "")
        return {
            "ok": True,
            "task_id": task_id,
            "task_number": entry.get("任务编号"),
            "retry_count": entry["retry_count"],
            "message": f"已按失败原因「{entry['上次失败原因']}」重新排队",
        }

    def recover_interrupted_plan(self, plan_key: str) -> dict[str, Any]:
        """中断恢复：按计划版本和稳定步骤键只续做未完成任务。

        - 已完成 / 已取消：保留清晰终态，不再入队
        - 执行失败：保留失败终态与原因，走显式重试，不自动重新入队
        - 执行中（中断在途）：回退为排队中等待重新调度
        - 清单存在但排队项缺失（中断在生成阶段）：按步骤边界补建
        """
        plan = store.execution_plans.get_plan(plan_key)
        if plan is None:
            return {"ok": False, "error": f"执行计划 {plan_key} 不存在"}

        requeued: list[int] = []
        rebuilt: list[int] = []
        with self._idempotency_lock:
            existing = {t["step_key"]: t for t in self._plan_tasks(plan_key)}
            for step in plan.manifest:
                task = existing.get(step.step_key)
                if task is None:
                    entry = self._build_task_entry(plan, step)
                    store.rows(self.MODULE).append(entry)
                    rebuilt.append(entry["id"])
                    continue
                if task.get("status") == "执行中":
                    task["status"] = "排队中"
                    task["当前状态"] = "排队中"
                    task["pending"] = True
                    requeued.append(task["id"])
                # 已完成/已取消/执行失败/排队中 均维持原状态

        progress = self.plan_progress(plan_key)
        return {
            "ok": True,
            "plan_key": plan_key,
            "requeued_in_flight": requeued,
            "rebuilt_missing": rebuilt,
            "remaining_task_ids": [
                b["任务编号"] for b in progress["boundaries"]
                if b["status"] in ACTIVE_STATUSES
            ],
            "progress": progress,
        }

    def cancel_plan(self, plan_key: str, reason: str = "人工取消执行计划") -> dict[str, Any]:
        """取消整个计划的未执行项，已完成项保留终态。"""
        plan = store.execution_plans.get_plan(plan_key)
        if plan is None:
            return {"ok": False, "error": f"执行计划 {plan_key} 不存在"}
        canceled = []
        for task in self._plan_tasks(plan_key):
            if task.get("status") in ACTIVE_STATUSES:
                self._mark_terminal(task, "已取消", reason=reason)
                canceled.append(task["id"])
        store.execution_plans.mark_canceled(plan_key, reason)
        return {"ok": True, "plan_key": plan_key, "canceled": canceled}

    def schedule_next_batch(self, max_tasks: int = 5) -> dict[str, Any]:
        """从排队中取下一批任务标记为执行中，调度前核对版本快照。

        模拟调度器的取任务逻辑 — 按优先级 + 提交时间排序。

        Returns:
            {"scheduled": int, "task_ids": [...], "snapshot_rejected": [...]}
        """
        rows = store.rows(self.MODULE)
        queued = [
            r for r in rows
            if r.get("status") == "排队中" and not r.get("abnormal")
        ]
        # 按优先级排序（高 > 中 > 低）
        priority_order = {"高": 0, "中": 1, "低": 2}
        queued.sort(key=lambda r: priority_order.get(r.get("优先级", "低"), 99))

        scheduled = []
        snapshot_rejected = []
        for task in queued[:max_tasks]:
            verification = self.verify_task_snapshot(task["id"])
            if not verification["ok"]:
                # 版本快照与当前计划不一致的处理项不参与调度
                task["snapshot_verified"] = False
                snapshot_rejected.append(task["id"])
                continue
            task["status"] = "执行中"
            task["当前状态"] = "执行中"
            task["pending"] = True
            task["开始执行时间"] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S")
            scheduled.append(task["id"])
            # 模拟执行记录
            store.channel.publish(
                topic="rpa.task_started",
                source=self.MODULE,
                payload={
                    "task_id": task["id"],
                    "task_number": task.get("任务编号"),
                    "plan_key": task.get("plan_key"),
                    "step_key": task.get("step_key"),
                    "start_time": task["开始执行时间"],
                },
            )

        return {
            "scheduled": len(scheduled),
            "task_ids": scheduled,
            "snapshot_rejected": snapshot_rejected,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 通道消费统计
        consumed = store.channel.snapshot(topic="rpa.steps_submitted")
        completed = store.channel.snapshot(topic="rpa.task_completed")
        plans = store.execution_plans.list_plans()
        return {
            **base,
            "steps_plans_consumed": len(consumed),
            "tasks_completed": len(completed),
            "registered_plans": len(plans),
            "failed_tasks": len(self.list_failed_tasks()),
            "plan_boundaries": [
                {
                    "plan_key": p.plan_key,
                    "bot_version": p.bot_version,
                    "remaining": self.plan_progress(p.plan_key)["remaining"],
                }
                for p in plans
            ],
        }
