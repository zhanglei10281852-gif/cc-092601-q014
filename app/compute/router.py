from __future__ import annotations

from fastapi import APIRouter, Query

from app.compute.schemas import (
    BatchOperation, CancelRequest, DecisionRequest, ForceTerminateRequest, InterventionRequestCreate,
    PolicyUpdate, RejectRequest, RetryRequest, RevokeRequest, TaskClaim, TaskFailure, TaskResult,
    TaskSubmit, TemplateCreate, PriorityRequest, QuotaSet, WithdrawResultRequest,
)
from app.compute.service import ComputeOperationsService

router = APIRouter(prefix="/api/compute", tags=["科学计算任务运营"])


def service() -> ComputeOperationsService:
    return ComputeOperationsService()


@router.get("/templates")
def list_templates():
    return {"items": service().list_templates()}


@router.post("/templates", status_code=201)
def create_template(payload: TemplateCreate, actor: str = Query(..., min_length=1)):
    return service().create_template(payload.model_dump(), actor)


@router.put("/quotas")
def set_quota(payload: QuotaSet, actor: str = Query(..., min_length=1)):
    return service().set_quota(payload.model_dump(), actor)


@router.post("/tasks", status_code=202)
def submit_task(payload: TaskSubmit):
    return service().submit(payload.model_dump())


@router.get("/tasks")
def list_tasks(status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = Query(default=100, ge=1, le=500)):
    return {"items": service().list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=limit)}


@router.get("/task-details/{task_id}")
def get_task(task_id: int):
    return service().get_task(task_id)


@router.post("/tasks/claim")
def claim_task(payload: TaskClaim):
    return {"task": service().claim(payload.worker_id, payload.capabilities, payload.lease_seconds)}


@router.post("/tasks/{task_id}/heartbeat")
def heartbeat(task_id: int, payload: TaskClaim):
    return service().heartbeat(task_id, payload.worker_id, payload.lease_seconds)


@router.post("/tasks/{task_id}/complete")
def complete_task(task_id: int, payload: TaskResult):
    return service().complete(task_id, payload.worker_id, payload.result, payload.metrics)


@router.post("/tasks/{task_id}/fail")
def fail_task(task_id: int, payload: TaskFailure):
    return service().fail(task_id, payload.worker_id, payload.error_code, payload.message, payload.retryable)


@router.post("/tasks/{task_id}/cancel")
def cancel_task(task_id: int, payload: CancelRequest):
    return service().cancel(task_id, payload.actor, payload.reason)


@router.post("/tasks/{task_id}/retry")
def retry_task(task_id: int, payload: RetryRequest):
    return service().retry(task_id, payload.actor, payload.reason, payload.priority)


@router.post("/tasks/{task_id}/priority")
def set_priority(task_id: int, payload: PriorityRequest):
    return service().set_priority(task_id, payload.actor, payload.reason, payload.priority, idempotency_key=payload.idempotency_key)


@router.post("/tasks/{task_id}/force-terminate")
def force_terminate(task_id: int, payload: ForceTerminateRequest):
    svc = service()
    if payload.idempotency_key:
        return svc.request_intervention(
            {"action": "force_terminate", "task_ids": [task_id], "actor": payload.actor, "reason": payload.reason, "idempotency_key": payload.idempotency_key},
            payload.actor,
        )
    return svc.force_terminate_request_or_execute(task_id, payload.actor, payload.reason)


@router.post("/tasks/{task_id}/withdraw-result")
def withdraw_result(task_id: int, payload: WithdrawResultRequest):
    svc = service()
    if payload.idempotency_key:
        return svc.request_intervention(
            {"action": "result_withdraw", "task_ids": [task_id], "actor": payload.actor, "reason": payload.reason, "idempotency_key": payload.idempotency_key},
            payload.actor,
        )
    return svc.withdraw_result_request_or_execute(task_id, payload.actor, payload.reason)


@router.post("/tasks/batch")
def batch_operation(payload: BatchOperation):
    return service().batch_operation(payload.model_dump())


# ----- 高风险干预审批策略 -----

@router.get("/approval-policies")
def list_approval_policies():
    return {"items": service().list_approval_policies()}


@router.put("/approval-policies/{action}")
def update_approval_policy(action: str, payload: PolicyUpdate, actor: str = Query(..., min_length=1)):
    return service().update_approval_policy(action, payload.model_dump(), actor)


# ----- 高风险干预申请与复核 -----

@router.post("/intervention-requests", status_code=201)
def create_intervention_request(payload: InterventionRequestCreate):
    data = payload.model_dump()
    actor = data.pop("actor")
    return service().request_intervention(data, actor)


@router.get("/intervention-requests")
def list_intervention_requests(status: str | None = None, action: str | None = None, limit: int = Query(default=100, ge=1, le=500)):
    return service().list_intervention_requests(status=status, action=action, limit=limit)


@router.get("/intervention-requests/{request_id}")
def get_intervention_request(request_id: int):
    return service().get_intervention_request(request_id)


@router.post("/intervention-requests/{request_id}/approve")
def approve_intervention_request(request_id: int, payload: DecisionRequest):
    return service().approve_request(request_id, payload.reviewer, payload.reason)


@router.post("/intervention-requests/{request_id}/reject")
def reject_intervention_request(request_id: int, payload: RejectRequest):
    return service().reject_request(request_id, payload.reviewer, payload.reason)


@router.post("/intervention-requests/{request_id}/revoke")
def revoke_intervention_request(request_id: int, payload: RevokeRequest):
    return service().revoke_request(request_id, payload.actor, payload.reason)


@router.post("/recovery/expired-leases")
def recover_expired(actor: str = Query(default="recovery-worker", min_length=1)):
    return service().recover_expired(actor)


@router.get("/summary")
def summary():
    return service().summary()
