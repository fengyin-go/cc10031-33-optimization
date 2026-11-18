"""执行计划与任务队列专属接口：幂等编排、版本冲突处理、中断恢复。

与通用资源路由分工：
- /api/execution-plans/... 承载执行计划语义（提交/边界/恢复/失败重试）
- /api/task_queue 继续走通用资源路由（列表、动作、导出），并已新增
  执行计划/流程版本/步骤键过滤字段
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.step_editor import StepEditorService
from app.services.task_queue import TaskQueueService
from app.store import store

router = APIRouter(prefix="/api/execution-plans", tags=["执行计划与任务队列"])


class StepPayload(BaseModel):
    seq: int
    name: str
    type: str = "custom"
    element: str = ""
    params: str = ""
    wait: str = "auto"
    on_error: str = "retry"


class PlanSubmitPayload(BaseModel):
    bot_id: int
    steps: list[StepPayload] = Field(default_factory=list)
    bot_version: str | None = None
    conflict_policy: str = "cancel_old"  # cancel_old | coexist


@router.post("/submit")
def submit_plan(payload: PlanSubmitPayload) -> dict[str, Any]:
    """业务入口：提交执行计划并联动刷新待办（消费通道生成幂等排队项）。

    重复提交同一计划（同流程 + 同版本）复用任务编号，只算一次；
    并发两次提交由服务端复合唯一键 + 锁保证只生成一份排队项。
    """
    step_svc = StepEditorService()
    task_svc = TaskQueueService()

    steps_data = [step.model_dump() for step in payload.steps]
    submit = step_svc.submit_steps_to_queue(
        bot_id=payload.bot_id,
        steps_data=steps_data,
        bot_version=payload.bot_version,
    )
    if submit.get("error"):
        return {"ok": False, "error": submit["error"], "submit": submit}

    queue = task_svc.consume_steps_to_queue(conflict_policy=payload.conflict_policy)
    # 注册表对账兜底：通道消息缺失或已消费时，仍以复合唯一键保证幂等
    resync = task_svc.resync_plans_to_queue(conflict_policy=payload.conflict_policy)
    plan_key = submit["plan_key"]
    return {
        "ok": True,
        "submit": submit,
        "queue": queue,
        "resync": resync,
        # 联动刷新的待办与完成边界
        "todo": task_svc.plan_progress(plan_key),
    }


@router.get("")
def list_plans(bot_id: int | None = None) -> dict[str, Any]:
    """按流程版本和计划查看步骤完成边界（任务队列总览）。"""
    task_svc = TaskQueueService()
    boundaries = task_svc.list_plan_boundaries(bot_id)
    return {"count": len(boundaries), "plans": boundaries}


@router.get("/{plan_key}/progress")
def plan_progress(plan_key: str) -> dict[str, Any]:
    """单个计划的步骤完成边界：恢复后只出现未完成项，终态清晰保留。"""
    return TaskQueueService().plan_progress(plan_key)


@router.post("/{plan_key}/recover")
def recover_plan(plan_key: str) -> dict[str, Any]:
    """中断恢复：按计划版本和步骤键只续做未完成任务。"""
    return TaskQueueService().recover_interrupted_plan(plan_key)


@router.post("/{plan_key}/cancel")
def cancel_plan(plan_key: str, reason: str = "人工取消执行计划") -> dict[str, Any]:
    """取消计划未执行项（已完成项保留终态）。"""
    return TaskQueueService().cancel_plan(plan_key, reason=reason)


@router.get("/failures/list")
def list_failures() -> dict[str, Any]:
    """失败清单；空态返回空列表，表示暂无失败。"""
    failed = TaskQueueService().list_failed_tasks()
    return {"count": len(failed), "failures": failed}


@router.post("/tasks/{task_id}/retry")
def retry_failed(task_id: int) -> dict[str, Any]:
    """重试中断异常任务，必须携带并展示失败原因。"""
    return TaskQueueService().retry_failed_task(task_id)


@router.get("/tasks/{task_id}/snapshot")
def task_snapshot(task_id: int) -> dict[str, Any]:
    """核对处理项对应的流程版本快照。"""
    return TaskQueueService().verify_task_snapshot(task_id)
