"""执行计划幂等与失败恢复测试。

覆盖：
- 重复提交同一计划复用任务编号，不产生重复排队项
- 并发两次提交只算一次（复合唯一键 + 锁）
- 不同流程版本计划的排队项可区分（plan_key + step_key 复合唯一）
- 中断恢复只续做未完成任务，已完成/已取消不重新入队
- 同流程多版本计划的两种处理边界（取消旧版本 / 共存）
- 流程版本快照核对、失败原因与重试
"""
from __future__ import annotations

import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.main import app  # noqa: E402
from app.services.step_editor import StepEditorService  # noqa: E402
from app.services.task_queue import TaskQueueService  # noqa: E402
from app.store import store  # noqa: E402


TEST_BOT_ID = 9001
STEPS = [
    {"seq": 1, "name": "打开目标页面", "type": "navigate", "element": "#login-btn"},
    {"seq": 2, "name": "输入关键词", "type": "input", "element": "#search-input"},
    {"seq": 3, "name": "点击搜索", "type": "click", "element": "#search-btn"},
    {"seq": 4, "name": "提取结果表格", "type": "extract", "element": ".results-table"},
    {"seq": 5, "name": "翻页", "type": "click", "element": ".next-page"},
]


@pytest.fixture
def isolated_queue():
    """隔离测试数据：仅清理本测试产生的计划与排队任务。"""
    task_rows = store.rows("task_queue")
    step_rows = store.rows("step_editor")
    task_before = len(task_rows)
    step_before = len(step_rows)
    plan_before = {p.plan_key for p in store.execution_plans.list_plans()}
    yield
    # 清理测试新增的排队任务
    del task_rows[task_before:]
    # 清理测试新增的步骤
    del step_rows[step_before:]
    for plan in store.execution_plans.list_plans():
        if plan.plan_key not in plan_before:
            store.execution_plans._plans.pop(plan.plan_key, None)


def _submit_and_consume(bot_id=TEST_BOT_ID, version="1.0.0",
                         steps=None, conflict_policy="cancel_old"):
    step_svc = StepEditorService()
    task_svc = TaskQueueService()
    submit = step_svc.submit_steps_to_queue(
        bot_id=bot_id, steps_data=steps or STEPS, bot_version=version,
    )
    queue = task_svc.consume_steps_to_queue(conflict_policy=conflict_policy)
    return submit, queue


class TestIdempotentSubmission:
    def test_repeat_submission_reuses_task_numbers(self, isolated_queue):
        """重复提交同一计划：任务编号只由流程和步骤序号拼接并复用。"""
        submit1, queue1 = _submit_and_consume()
        assert submit1["reused"] is False
        assert queue1["generated"] == 5
        first_tasks = [
            t for t in store.rows("task_queue")
            if t.get("plan_key") == submit1["plan_key"]
        ]
        numbers = [t["任务编号"] for t in first_tasks]
        assert numbers == [f"TASK-{TEST_BOT_ID}-{i:03d}" for i in range(1, 6)]

        # 再次提交同一计划
        submit2, queue2 = _submit_and_consume()
        assert submit2["reused"] is True
        assert submit2["plan_key"] == submit1["plan_key"]
        assert queue2["generated"] == 0
        assert queue2["reused"] == 5

        # 队列中仍只有 5 条该计划的排队项，没有重复排队项
        same_plan = [
            t for t in store.rows("task_queue")
            if t.get("plan_key") == submit1["plan_key"]
        ]
        assert len(same_plan) == 5
        # 任务编号保持不变
        assert sorted(t["任务编号"] for t in same_plan) == sorted(numbers)

    def test_concurrent_double_submission_counts_once(self, isolated_queue):
        """并发两次提交只算一次。"""
        barrier = threading.Barrier(2)

        def worker():
            barrier.wait()
            _submit_and_consume()

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(worker) for _ in range(2)]
            [f.result(timeout=10) for f in futures]

        plan_key = store.execution_plans.build_plan_key(TEST_BOT_ID, "1.0.0")
        tasks = [t for t in store.rows("task_queue") if t.get("plan_key") == plan_key]
        assert len(tasks) == 5
        # 复合唯一键不允许出现重复 (plan_key, step_key)
        composite = {(t["plan_key"], t["step_key"]) for t in tasks}
        assert len(composite) == 5

    def test_step_keys_are_stable_and_composite_unique(self, isolated_queue):
        """执行计划与稳定步骤键共同唯一。"""
        submit, _ = _submit_and_consume()
        plan = store.execution_plans.get_plan(submit["plan_key"])
        assert [s.step_key for s in plan.manifest] == [
            store.execution_plans.build_step_key(TEST_BOT_ID, seq)
            for seq in range(1, 6)
        ]
        # 中断恢复后再重复消费也不会出现重复边界
        TaskQueueService().recover_interrupted_plan(submit["plan_key"])
        TaskQueueService().consume_steps_to_queue()
        tasks = [t for t in store.rows("task_queue") if t.get("plan_key") == submit["plan_key"]]
        assert len(tasks) == 5


class TestVersionIsolation:
    def test_two_versions_get_distinguishable_queue_items(self, isolated_queue):
        """同一流程两个版本计划的排队项靠 plan_key/step_key 区分，不互相混淆。"""
        submit_v1, _ = _submit_and_consume(version="1.0.0")
        submit_v2, _ = _submit_and_consume(
            version="2.0.0", conflict_policy="coexist",
        )
        assert submit_v1["plan_key"] != submit_v2["plan_key"]

        v1 = [t for t in store.rows("task_queue") if t.get("plan_key") == submit_v1["plan_key"]]
        v2 = [t for t in store.rows("task_queue") if t.get("plan_key") == submit_v2["plan_key"]]
        assert len(v1) == 5 and len(v2) == 5
        # 任务编号相同（只由流程+序号拼接），但排队项通过计划键可区分
        assert {t["任务编号"] for t in v1} == {t["任务编号"] for t in v2}
        assert {t["执行计划"] for t in v1}.isdisjoint({t["执行计划"] for t in v2})
        # 两个计划各自完成，步骤键不混淆
        svc = TaskQueueService()
        for task in v1:
            assert svc.update_queue_status(task["id"], "已完成") is not None
        p1 = svc.plan_progress(submit_v1["plan_key"])
        p2 = svc.plan_progress(submit_v2["plan_key"])
        assert p1["completed"] == 5 and p1["remaining"] == 0
        assert p2["completed"] == 0 and p2["remaining"] == 5

    def test_cancel_old_policy_cancels_superseded_version(self, isolated_queue):
        """最新计划版本可取消旧版本未执行项，已完成项保留终态。"""
        svc = TaskQueueService()
        submit_v1, _ = _submit_and_consume(version="1.0.0")
        # 旧版本先完成两步
        scheduled = svc.schedule_next_batch(max_tasks=2)
        for task_id in scheduled["task_ids"]:
            svc.update_queue_status(task_id, "已完成")

        submit_v2, queue_v2 = _submit_and_consume(version="2.0.0")
        assert queue_v2["canceled_by_conflict"] == 3

        old_plan = store.execution_plans.get_plan(submit_v1["plan_key"])
        assert old_plan.canceled is True
        p1 = svc.plan_progress(submit_v1["plan_key"])
        assert p1["completed"] == 2       # 已完成保留清晰终态
        assert p1["canceled"] == 3        # 未执行项被取消
        assert p1["remaining"] == 0
        # 新版本计划照常排队
        p2 = svc.plan_progress(submit_v2["plan_key"])
        assert p2["remaining"] == 5

    def test_coexist_policy_keeps_both_plans_intact(self, isolated_queue):
        """两套处理边界都不会混淆两个计划的步骤键。"""
        svc = TaskQueueService()
        submit_v1, _ = _submit_and_consume(version="1.0.0", conflict_policy="coexist")
        submit_v2, queue_v2 = _submit_and_consume(
            version="2.0.0", conflict_policy="coexist",
        )
        assert queue_v2["canceled_by_conflict"] == 0
        assert svc.plan_progress(submit_v1["plan_key"])["remaining"] == 5
        assert svc.plan_progress(submit_v2["plan_key"])["remaining"] == 5


class TestInterruptRecovery:
    def test_recovery_only_continues_unfinished_tasks(self, isolated_queue):
        """中断恢复不能把已完成步骤重新加入队列。"""
        svc = TaskQueueService()
        submit, _ = _submit_and_consume()
        plan_key = submit["plan_key"]

        # 模拟部分执行：前 2 步完成、第 3 步执行中（中断在途）、后 2 步排队
        scheduled = svc.schedule_next_batch(max_tasks=3)
        first_two = scheduled["task_ids"][:2]
        in_flight = scheduled["task_ids"][2]
        for task_id in first_two:
            svc.update_queue_status(task_id, "已完成")

        recovered = svc.recover_interrupted_plan(plan_key)
        assert recovered["requeued_in_flight"] == [in_flight]
        assert recovered["rebuilt_missing"] == []

        progress = svc.plan_progress(plan_key)
        assert progress["completed"] == 2
        assert progress["remaining"] == 3
        # 恢复后的列表只出现未完成项
        unfinished = [b for b in progress["boundaries"] if b["status"] in ("排队中", "执行中")]
        assert {b["seq"] for b in unfinished} == {3, 4, 5}
        finished = [b for b in progress["boundaries"] if b["status"] == "已完成"]
        assert {b["seq"] for b in finished} == {1, 2}
        # 队列条目总数不变，没有重复入队
        assert len(svc._plan_tasks(plan_key)) == 5

    def test_recovery_rebuilds_missing_boundaries(self, isolated_queue):
        """中断发生在任务生成阶段：按处理项边界补建缺失任务。"""
        svc = TaskQueueService()
        submit, _ = _submit_and_consume()
        plan_key = submit["plan_key"]
        # 人为删掉两条排队项，模拟生成阶段中断
        rows = store.rows("task_queue")
        removed = [t for t in svc._plan_tasks(plan_key)][-2:]
        for task in removed:
            rows.remove(task)

        recovered = svc.recover_interrupted_plan(plan_key)
        assert len(recovered["rebuilt_missing"]) == 2
        assert len(svc._plan_tasks(plan_key)) == 5
        rebuilt = svc._plan_tasks(plan_key)
        assert sorted(t["seq"] for t in rebuilt) == [1, 2, 3, 4, 5]

    def test_canceled_tasks_keep_terminal_state_on_recovery(self, isolated_queue):
        """已取消任务保留清晰终态，恢复不会重新入队。"""
        svc = TaskQueueService()
        submit, _ = _submit_and_consume()
        plan_key = submit["plan_key"]
        target = svc._plan_tasks(plan_key)[0]
        svc.update_queue_status(target["id"], "已取消", failure_reason="人工取消")

        recovered = svc.recover_interrupted_plan(plan_key)
        assert recovered["requeued_in_flight"] == []
        task = store.find("task_queue", target["id"])
        assert task["status"] == "已取消"
        assert task["pending"] is False

    def test_final_verification_only_unfinished_remain(self, isolated_queue):
        """最终核对：中断后的任务数据只续做未完成步骤。"""
        svc = TaskQueueService()
        submit, _ = _submit_and_consume()
        plan_key = submit["plan_key"]
        scheduled = svc.schedule_next_batch(max_tasks=5)
        for task_id in scheduled["task_ids"][:3]:
            svc.update_queue_status(task_id, "已完成")
        # 中断、恢复
        svc.recover_interrupted_plan(plan_key)
        progress = svc.plan_progress(plan_key)
        remaining_seqs = sorted(
            b["seq"] for b in progress["boundaries"]
            if b["status"] in ("排队中", "执行中")
        )
        assert remaining_seqs == [4, 5]
        assert progress["completion_boundary_seq"] == 3


class TestSnapshotAndFailure:
    def test_snapshot_verification_passes_then_detects_drift(self, isolated_queue):
        """任务队列核对处理项对应的流程版本快照。"""
        svc = TaskQueueService()
        submit, _ = _submit_and_consume()
        task = svc._plan_tasks(submit["plan_key"])[0]
        result = svc.verify_task_snapshot(task["id"])
        assert result["ok"] is True
        assert result["mismatch_fields"] == []

        # 计划快照发生变化（模拟版本编排漂移）
        plan = store.execution_plans.get_plan(submit["plan_key"])
        plan.snapshot["目标系统"] = "被篡改的目标系统"
        drifted = svc.verify_task_snapshot(task["id"])
        assert drifted["ok"] is False
        assert "目标系统" in drifted["mismatch_fields"]

        # 调度时快照不一致的处理项被拒绝
        scheduled = svc.schedule_next_batch(max_tasks=5)
        assert task["id"] in scheduled["snapshot_rejected"]
        assert task["status"] == "排队中"

    def test_failure_records_reason_and_retry_requeues(self, isolated_queue):
        """重试中断异常必须说明原因。"""
        svc = TaskQueueService()
        submit, _ = _submit_and_consume()
        task = svc._plan_tasks(submit["plan_key"])[0]
        reason = "元素定位超时：#login-btn 在 30s 内未出现"
        svc.update_queue_status(task["id"], "执行失败", failure_reason=reason)

        failures = svc.list_failed_tasks()
        assert len(failures) == 1
        assert failures[0]["失败原因"] == reason
        # 平台失败恢复记录同步留痕
        assert store.recovery.pending_count() >= 1

        retry = svc.retry_failed_task(task["id"])
        assert retry["ok"] is True
        refreshed = store.find("task_queue", task["id"])
        assert refreshed["status"] == "排队中"
        assert refreshed["retry_count"] == 1
        assert refreshed["上次失败原因"] == reason

    def test_retry_requires_failure_reason(self, isolated_queue):
        svc = TaskQueueService()
        submit, _ = _submit_and_consume()
        task = svc._plan_tasks(submit["plan_key"])[0]
        # 直接置失败但不给原因（绕过 update_queue_status 的异常路径）
        task["status"] = "执行失败"
        task["失败原因"] = ""
        result = svc.retry_failed_task(task["id"])
        assert result["ok"] is False
        assert "失败原因" in result["error"]

    def test_retry_non_failed_task_rejected(self, isolated_queue):
        svc = TaskQueueService()
        submit, _ = _submit_and_consume()
        task = svc._plan_tasks(submit["plan_key"])[0]
        result = svc.retry_failed_task(task["id"])
        assert result["ok"] is False
        assert "仅执行失败" in result["error"]

    def test_failure_empty_state(self, isolated_queue):
        """空态暂无失败。"""
        svc = TaskQueueService()
        submit, _ = _submit_and_consume()
        failures = svc.list_failed_tasks()
        assert failures == []


class TestPlanApi:
    def test_submit_endpoint_refreshes_todo(self, isolated_queue):
        """业务入口提交后联动刷新待办与完成边界。"""
        client = TestClient(app)
        payload = {"bot_id": TEST_BOT_ID, "steps": STEPS, "bot_version": "1.0.0"}
        resp = client.post("/api/execution-plans/submit", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["queue"]["generated"] == 5
        assert data["todo"]["remaining"] == 5
        assert data["todo"]["total_steps"] == 5

        # 重复提交只算一次
        resp2 = client.post("/api/execution-plans/submit", json=payload)
        data2 = resp2.json()
        assert data2["submit"]["reused"] is True
        assert data2["queue"]["generated"] == 0

    def test_progress_and_recovery_endpoints(self, isolated_queue):
        client = TestClient(app)
        plan_key = store.execution_plans.build_plan_key(TEST_BOT_ID, "1.0.0")
        client.post(
            "/api/execution-plans/submit",
            json={"bot_id": TEST_BOT_ID, "steps": STEPS, "bot_version": "1.0.0"},
        )
        svc = TaskQueueService()
        scheduled = svc.schedule_next_batch(max_tasks=2)
        for task_id in scheduled["task_ids"]:
            svc.update_queue_status(task_id, "已完成")

        resp = client.get(f"/api/execution-plans/{plan_key}/progress")
        assert resp.json()["completion_boundary_seq"] == 2

        resp = client.post(f"/api/execution-plans/{plan_key}/recover")
        body = resp.json()
        assert body["ok"] is True
        assert body["progress"]["remaining"] == 3

    def test_queue_filter_by_plan_and_version(self, isolated_queue):
        """任务队列可按流程版本和计划过滤。"""
        client = TestClient(app)
        client.post(
            "/api/execution-plans/submit",
            json={"bot_id": TEST_BOT_ID, "steps": STEPS, "bot_version": "1.0.0"},
        )
        plan_key = store.execution_plans.build_plan_key(TEST_BOT_ID, "1.0.0")
        resp = client.get(f"/api/task_queue?执行计划={plan_key}")
        items = resp.json()["items"]
        assert len(items) == 5
        assert all(item["执行计划"] == plan_key for item in items)
        resp2 = client.get("/api/task_queue?流程版本=1.0.0")
        assert all(item["流程版本"] == "1.0.0" for item in resp2.json()["items"])
