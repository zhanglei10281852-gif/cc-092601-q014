from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.core.errors import ConflictError
from app.database import get_connection


TEMPLATE = {
    "code": "solver-a",
    "name": "方程求解模板",
    "algorithm": "solver-a",
    "parameter_schema": {
        "iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000},
        "mode": {"type": "string", "required": True, "choices": ["fast", "accurate"]},
    },
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}


def submit_payload(key: str, *, user: str = "researcher-1", priority: int = 50) -> dict:
    return {
        "template_code": "solver-a",
        "project_code": "project-a",
        "requested_by": user,
        "parameters": {"iterations": 100, "mode": "accurate"},
        "priority": priority,
        "idempotency_key": key,
    }


def create_template(client) -> None:
    response = client.post("/api/compute/templates?actor=administrator", json=TEMPLATE)
    assert response.status_code == 201, response.text


def make_task(client, key: str, *, priority: int = 50) -> dict:
    response = client.post("/api/compute/tasks", json=submit_payload(key, priority=priority))
    assert response.status_code == 202, response.text
    return response.json()


def claim_task(client, task_id: int, worker: str = "worker-1") -> dict:
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": worker, "capabilities": ["solver-a"], "lease_seconds": 60})
    assert claimed.status_code == 200 and claimed.json()["task"]["id"] == task_id
    return claimed.json()["task"]


def make_failed_task(client, key: str, worker: str = "worker-1") -> dict:
    task = make_task(client, key)
    claim_task(client, task["id"], worker)
    failed = client.post(f"/api/compute/tasks/{task['id']}/fail", json={"worker_id": worker, "error_code": "boom", "message": "计算崩溃", "retryable": False})
    assert failed.status_code == 200 and failed.json()["status"] == "failed"
    return failed.json()


def make_succeeded_task(client, key: str, worker: str = "worker-1") -> dict:
    task = make_task(client, key)
    claim_task(client, task["id"], worker)
    completed = client.post(f"/api/compute/tasks/{task['id']}/complete", json={"worker_id": worker, "result": {"value": 1.5}, "metrics": {}})
    assert completed.status_code == 200 and completed.json()["status"] == "succeeded"
    return completed.json()


def test_priority_promotion_over_threshold_requires_second_person(client):
    create_template(client)
    task = make_task(client, "approval-priority-1")
    direct = client.post(f"/api/compute/tasks/{task['id']}/priority", json={"actor": "admin-a", "reason": "常规调整", "priority": 99})
    assert direct.status_code == 200 and direct.json()["priority"] == 99

    outcome = client.post(f"/api/compute/tasks/{task['id']}/priority", json={"actor": "admin-a", "reason": "关键项目冲刺", "priority": 100})
    assert outcome.status_code == 202
    body = outcome.json()
    assert body["approval_required"] is True
    request = body["request"]
    assert request["status"] == "pending"
    assert request["action"] == "priority"
    assert request["applicant"] == "admin-a"
    assert request["payload"] == {"priority": 100, "task_ids": [task["id"]]}
    assert request["task_refs"] == [{"task_id": task["id"], "version": 2}]
    assert client.get(f"/api/compute/task-details/{task['id']}").json()["priority"] == 99

    self_review = client.post(f"/api/compute/approvals/{request['id']}/decide", json={"approver": "admin-a", "decision": "approve"})
    assert self_review.status_code == 403

    decided = client.post(f"/api/compute/approvals/{request['id']}/decide", json={"approver": "admin-b", "decision": "approve", "reason": "确认紧急"})
    assert decided.status_code == 200
    decided_body = decided.json()
    assert decided_body["duplicate"] is False
    assert decided_body["request"]["status"] == "approved"
    assert decided_body["request"]["decision_actor"] == "admin-b"
    execution = decided_body["request"]["execution"]
    assert execution["tasks"] == [{"status": "queued", "task_id": task["id"], "version": 3}]
    assert len(execution["intervention_ids"]) == 1

    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert details["priority"] == 100
    assert [item["action"] for item in details["interventions"]] == ["priority", "priority"]
    assert details["interventions"][-1]["batch_key"] == f"approval:{request['id']}"
    assert details["interventions"][-1]["actor"] == "admin-b"

    repeated = client.post(f"/api/compute/approvals/{request['id']}/decide", json={"approver": "admin-c", "decision": "reject", "reason": "重复决定"})
    assert repeated.status_code == 200
    assert repeated.json()["duplicate"] is True
    assert repeated.json()["request"]["status"] == "approved"
    assert repeated.json()["request"]["execution"] == execution
    assert len(client.get(f"/api/compute/task-details/{task['id']}").json()["interventions"]) == 2


def test_reject_requires_reason_and_returns_original_on_repeat(client):
    create_template(client)
    task = make_task(client, "approval-reject-1")
    outcome = client.post(f"/api/compute/tasks/{task['id']}/priority", json={"actor": "admin-a", "reason": "冲刺", "priority": 100})
    request_id = outcome.json()["request"]["id"]

    missing_reason = client.post(f"/api/compute/approvals/{request_id}/decide", json={"approver": "admin-b", "decision": "reject"})
    assert missing_reason.status_code == 422

    rejected = client.post(f"/api/compute/approvals/{request_id}/decide", json={"approver": "admin-b", "decision": "reject", "reason": "资源不足"})
    assert rejected.status_code == 200
    assert rejected.json()["request"]["status"] == "rejected"
    assert rejected.json()["request"]["decision_reason"] == "资源不足"
    assert client.get(f"/api/compute/task-details/{task['id']}").json()["priority"] == 50

    repeated = client.post(f"/api/compute/approvals/{request_id}/decide", json={"approver": "admin-c", "decision": "approve"})
    assert repeated.status_code == 200
    assert repeated.json()["duplicate"] is True
    assert repeated.json()["request"]["status"] == "rejected"

    revoke_after_decision = client.post(f"/api/compute/approvals/{request_id}/revoke", json={"actor": "admin-a"})
    assert revoke_after_decision.status_code == 409


def test_stale_snapshot_requires_rehearsal_before_approval(client):
    create_template(client)
    task = make_task(client, "approval-stale-1")
    outcome = client.post(f"/api/compute/tasks/{task['id']}/priority", json={"actor": "admin-a", "reason": "冲刺", "priority": 100})
    request_id = outcome.json()["request"]["id"]

    changed = client.post(f"/api/compute/tasks/{task['id']}/priority", json={"actor": "admin-b", "reason": "常规调整", "priority": 60})
    assert changed.status_code == 200

    stale_decision = client.post(f"/api/compute/approvals/{request_id}/decide", json={"approver": "admin-b", "decision": "approve"})
    assert stale_decision.status_code == 409
    assert client.get(f"/api/compute/approvals/{request_id}").json()["status"] == "stale"

    blocked = client.post(f"/api/compute/approvals/{request_id}/decide", json={"approver": "admin-b", "decision": "approve"})
    assert blocked.status_code == 409

    outsider = client.post(f"/api/compute/approvals/{request_id}/rehearse", json={"actor": "admin-b"})
    assert outsider.status_code == 403

    rehearsed = client.post(f"/api/compute/approvals/{request_id}/rehearse", json={"actor": "admin-a"})
    assert rehearsed.status_code == 200
    assert rehearsed.json()["status"] == "pending"
    assert rehearsed.json()["task_refs"] == [{"task_id": task["id"], "version": 2}]

    approved = client.post(f"/api/compute/approvals/{request_id}/decide", json={"approver": "admin-b", "decision": "approve"})
    assert approved.status_code == 200
    assert client.get(f"/api/compute/task-details/{task['id']}").json()["priority"] == 100


def test_revoke_only_by_applicant_and_idempotent(client):
    create_template(client)
    task = make_task(client, "approval-revoke-1")
    outcome = client.post(f"/api/compute/tasks/{task['id']}/priority", json={"actor": "admin-a", "reason": "冲刺", "priority": 100})
    request_id = outcome.json()["request"]["id"]

    outsider = client.post(f"/api/compute/approvals/{request_id}/revoke", json={"actor": "admin-b"})
    assert outsider.status_code == 403

    revoked = client.post(f"/api/compute/approvals/{request_id}/revoke", json={"actor": "admin-a"})
    assert revoked.status_code == 200 and revoked.json()["status"] == "revoked"

    again = client.post(f"/api/compute/approvals/{request_id}/revoke", json={"actor": "admin-a"})
    assert again.status_code == 200 and again.json()["status"] == "revoked"

    decided = client.post(f"/api/compute/approvals/{request_id}/decide", json={"approver": "admin-b", "decision": "approve"})
    assert decided.status_code == 409
    assert client.get(f"/api/compute/task-details/{task['id']}").json()["priority"] == 50


def test_batch_retry_generates_single_request_and_audit_chain(client):
    create_template(client)
    first = make_failed_task(client, "approval-batch-1")
    second = make_failed_task(client, "approval-batch-2")

    outcome = client.post("/api/compute/tasks/batch", json={"task_ids": [first["id"], second["id"]], "operation": "retry", "actor": "admin-a", "reason": "故障恢复"})
    assert outcome.status_code == 202
    request = outcome.json()["request"]
    assert request["action"] == "batch_retry"
    assert len(request["task_refs"]) == 2
    assert client.get(f"/api/compute/task-details/{first['id']}").json()["status"] == "failed"

    decided = client.post(f"/api/compute/approvals/{request['id']}/decide", json={"approver": "admin-b", "decision": "approve"})
    assert decided.status_code == 200
    assert {item["task_id"] for item in decided.json()["request"]["execution"]["tasks"]} == {first["id"], second["id"]}

    audit = client.get(f"/api/compute/approvals/{request['id']}/audit")
    assert audit.status_code == 200
    chain = audit.json()
    assert chain["request"]["status"] == "approved"
    assert chain["request"]["applicant"] == "admin-a"
    assert chain["decision"] == {"actor": "admin-b", "at": chain["decision"]["at"], "outcome": "approved", "reason": ""}
    assert len(chain["interventions"]) == 2
    assert {item["action"] for item in chain["interventions"]} == {"retry"}
    assert all(item["batch_key"] == f"approval:{request['id']}" for item in chain["interventions"])
    assert {task["id"]: task["status"] for task in chain["tasks"]} == {first["id"]: "queued", second["id"]: "queued"}

    listing = client.get("/api/compute/approvals", params={"status": "approved", "applicant": "admin-a"})
    assert [item["id"] for item in listing.json()["items"]] == [request["id"]]


def test_force_terminate_requires_approval_and_clears_lease(client):
    create_template(client)
    task = make_task(client, "approval-force-1")
    claim_task(client, task["id"])

    outcome = client.post(f"/api/compute/tasks/{task['id']}/force-terminate", json={"actor": "admin-a", "reason": "工作者失联"})
    assert outcome.status_code == 202
    request = outcome.json()["request"]
    assert request["action"] == "force_terminate"
    assert client.get(f"/api/compute/task-details/{task['id']}").json()["status"] == "running"

    decided = client.post(f"/api/compute/approvals/{request['id']}/decide", json={"approver": "admin-b", "decision": "approve"})
    assert decided.status_code == 200
    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert details["status"] == "cancelled"
    assert details["lease_owner"] == ""
    assert details["interventions"][-1]["action"] == "force_terminate"


def test_result_withdraw_marks_result_and_requeues_task(client):
    create_template(client)
    task = make_succeeded_task(client, "approval-withdraw-1")

    outcome = client.post(f"/api/compute/tasks/{task['id']}/withdraw-result", json={"actor": "admin-a", "reason": "结果数据污染", "result_version": 1})
    assert outcome.status_code == 202
    request = outcome.json()["request"]
    assert request["action"] == "result_withdraw"

    decided = client.post(f"/api/compute/approvals/{request['id']}/decide", json={"approver": "admin-b", "decision": "approve"})
    assert decided.status_code == 200
    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert details["status"] == "queued"
    assert details["current_result_version"] is None
    assert details["results"][0]["withdrawn_by"] == "admin-b"
    assert details["results"][0]["withdraw_reason"] == "结果数据污染"
    assert details["results"][0]["withdrawn_at"] != ""
    assert details["interventions"][-1]["action"] == "result_withdraw"

    again = client.post(f"/api/compute/tasks/{task['id']}/withdraw-result", json={"actor": "admin-a", "reason": "重复撤回", "result_version": 1})
    assert again.status_code == 409


def test_rehearsal_rejects_inapplicable_requests_at_creation(client):
    create_template(client)
    failed_task = make_failed_task(client, "approval-guard-1")
    outcome = client.post(f"/api/compute/tasks/{failed_task['id']}/force-terminate", json={"actor": "admin-a", "reason": "非法终止"})
    assert outcome.status_code == 409
    assert client.get("/api/compute/approvals").json()["items"] == []

    succeeded = make_succeeded_task(client, "approval-guard-2")
    mismatch = client.post(f"/api/compute/tasks/{succeeded['id']}/withdraw-result", json={"actor": "admin-a", "reason": "版本不符", "result_version": 9})
    assert mismatch.status_code == 409

    queued = make_task(client, "approval-guard-3")
    batch = client.post("/api/compute/tasks/batch", json={"task_ids": [failed_task["id"], queued["id"]], "operation": "retry", "actor": "admin-a", "reason": "混合状态"})
    assert batch.status_code == 409
    assert client.get("/api/compute/approvals").json()["items"] == []


def test_policy_update_changes_routing_and_allows_direct_execution(client):
    create_template(client)
    policy = client.get("/api/compute/approval-policy")
    assert policy.status_code == 200
    assert policy.json()["priority_threshold"] == 100
    assert policy.json()["force_terminate_requires_approval"] == 1

    updated = client.put(
        "/api/compute/approval-policy?actor=administrator",
        json={"priority_threshold": 90, "force_terminate_requires_approval": False, "result_withdraw_requires_approval": False},
    )
    assert updated.status_code == 200
    assert updated.json()["priority_threshold"] == 90
    assert updated.json()["updated_by"] == "administrator"

    task = make_task(client, "approval-policy-1")
    promoted = client.post(f"/api/compute/tasks/{task['id']}/priority", json={"actor": "admin-a", "reason": "超过新阈值", "priority": 95})
    assert promoted.status_code == 202 and promoted.json()["request"]["status"] == "pending"

    claim_task(client, task["id"])
    terminated = client.post(f"/api/compute/tasks/{task['id']}/force-terminate", json={"actor": "admin-a", "reason": "直接终止"})
    assert terminated.status_code == 200 and terminated.json()["status"] == "cancelled"

    succeeded = make_succeeded_task(client, "approval-policy-2")
    withdrawn = client.post(f"/api/compute/tasks/{succeeded['id']}/withdraw-result", json={"actor": "admin-a", "reason": "直接撤回", "result_version": 1})
    assert withdrawn.status_code == 200 and withdrawn.json()["status"] == "queued"


def test_batch_priority_to_top_requires_approval(client):
    create_template(client)
    first = make_task(client, "approval-batch-priority-1", priority=40)
    second = make_task(client, "approval-batch-priority-2", priority=60)

    direct = client.post("/api/compute/tasks/batch", json={"task_ids": [first["id"], second["id"]], "operation": "priority", "actor": "admin-a", "reason": "常规批量", "priority": 99})
    assert direct.status_code == 200 and len(direct.json()["succeeded"]) == 2

    outcome = client.post("/api/compute/tasks/batch", json={"task_ids": [first["id"], second["id"]], "operation": "priority", "actor": "admin-a", "reason": "批量置顶", "priority": 100})
    assert outcome.status_code == 202
    request = outcome.json()["request"]
    assert request["action"] == "priority"
    assert len(request["task_refs"]) == 2

    decided = client.post(f"/api/compute/approvals/{request['id']}/decide", json={"approver": "admin-b", "decision": "approve"})
    assert decided.status_code == 200
    assert client.get(f"/api/compute/task-details/{first['id']}").json()["priority"] == 100
    assert client.get(f"/api/compute/task-details/{second['id']}").json()["priority"] == 100


def test_request_expires_after_configured_ttl(client):
    from app.database import init_db

    init_db()
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE, "administrator")
    service.update_policy({"request_ttl_seconds": 120}, "administrator")
    task = service.submit(submit_payload("approval-expire-1"))
    outcome = service.set_priority(task["id"], "admin-a", "紧急提升", 100)
    assert outcome["approval_required"] is True
    request_id = outcome["request"]["id"]
    assert outcome["request"]["status"] == "pending"

    clock.advance(seconds=121)
    assert service.get_request(request_id)["status"] == "expired"
    with pytest.raises(ConflictError):
        service.decide(request_id, "admin-b", "approve")
    with pytest.raises(ConflictError):
        service.revoke(request_id, "admin-a")
    with pytest.raises(ConflictError):
        service.rehearse(request_id, "admin-a")
    audit = service.approval_audit(request_id)
    assert audit["decision"]["outcome"] == "expired"
    assert service.get_task(task["id"])["priority"] == 50
