from __future__ import annotations

from datetime import UTC, datetime

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.database import get_connection

from tests.test_compute_operations import TEMPLATE, create_template, submit_payload


def make_task(client, key: str = "appr-000001", *, user: str = "researcher-1", priority: int = 50) -> int:
    response = client.post("/api/compute/tasks", json=submit_payload(key, user=user, priority=priority))
    assert response.status_code == 202, response.text
    return response.json()["id"]


def fail_task(client, task_id: int, *, worker: str = "w1", capability: str = "solver-a") -> None:
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": worker, "capabilities": [capability], "lease_seconds": 60})
    assert claimed.json()["task"]["id"] == task_id
    failed = client.post(
        f"/api/compute/tasks/{task_id}/fail",
        json={"worker_id": worker, "error_code": "fatal", "message": "不可恢复错误", "retryable": False},
    )
    assert failed.status_code == 200 and failed.json()["status"] == "failed"


def succeed_task(client, task_id: int, *, worker: str = "w1") -> None:
    client.post("/api/compute/tasks/claim", json={"worker_id": worker, "capabilities": ["solver-a"], "lease_seconds": 60})
    completed = client.post(
        f"/api/compute/tasks/{task_id}/complete",
        json={"worker_id": worker, "result": {"value": 1.0}, "metrics": {}},
    )
    assert completed.status_code == 200 and completed.json()["status"] == "succeeded"


# ----- 策略可配置 -----

def test_policies_are_seeded_and_configurable(client):
    policies = client.get("/api/compute/approval-policies").json()["items"]
    actions = {item["action"]: item for item in policies}
    assert set(actions) == {"priority_boost", "batch_retry", "force_terminate", "result_withdraw"}
    assert actions["priority_boost"]["enabled"] == 1
    updated = client.put(
        "/api/compute/approval-policies/priority_boost?actor=admin",
        json={"enabled": False, "priority_delta_threshold": 30, "batch_size_threshold": 3, "ttl_seconds": 600},
    )
    assert updated.status_code == 200
    assert updated.json()["enabled"] == 0 and updated.json()["priority_delta_threshold"] == 30
    assert client.put(
        "/api/compute/approval-policies/unknown?actor=admin",
        json={"enabled": True, "priority_delta_threshold": 1, "batch_size_threshold": 1, "ttl_seconds": 60},
    ).status_code == 422


def test_low_risk_operations_still_execute_directly(client):
    create_template(client)
    task_id = make_task(client, "low-risk-01")
    # 提升幅度低于阈值（默认 20）直接执行
    small = client.post(f"/api/compute/tasks/{task_id}/priority", json={"actor": "alice", "reason": "小幅调整", "priority": 60})
    assert small.status_code == 200
    assert "approval_required" not in small.json()
    assert small.json()["priority"] == 60
    # 取消仍是低风险直接执行
    assert client.post(f"/api/compute/tasks/{task_id}/cancel", json={"actor": "alice", "reason": "项目暂停"}).json()["status"] == "cancelled"
    # 单条失败重试（批量阈值为 2）直接执行
    failed_task = make_task(client, "low-risk-02")
    fail_task(client, failed_task, worker="w2")
    retried = client.post(f"/api/compute/tasks/{failed_task}/retry", json={"actor": "alice", "reason": "人工重试", "priority": 50})
    assert retried.status_code == 200 and retried.json()["status"] == "queued"


# ----- 超阈值提权 -----

def test_priority_boost_over_threshold_requires_approval_and_executes(client):
    create_template(client)
    task_id = make_task(client, "boost-01", priority=50)
    response = client.post(f"/api/compute/tasks/{task_id}/priority", json={"actor": "alice", "reason": "关键项目提权", "priority": 90})
    assert response.status_code == 200
    body = response.json()
    assert body["approval_required"] is True
    request_id = body["request"]["id"]
    assert body["request"]["status"] == "pending"
    assert body["request"]["snapshots"][0]["task_version"] >= 1
    assert body["request"]["snapshots"][0]["priority"] == 50
    # 提权未执行
    assert client.get(f"/api/compute/task-details/{task_id}").json()["priority"] == 50
    # 申请人不能复核自己的申请
    forbidden = client.post(f"/api/compute/intervention-requests/{request_id}/approve", json={"reviewer": "alice"})
    assert forbidden.status_code == 403
    # 复核人分离后批准，同事务执行
    approved = client.post(f"/api/compute/intervention-requests/{request_id}/approve", json={"reviewer": "bob", "reason": "确认关键项目"})
    assert approved.status_code == 200
    view = approved.json()
    assert view["status"] == "approved" and view["executed_at"]
    assert view["decisions"][0]["decision"] == "approved" and view["decisions"][0]["decided_by"] == "bob"
    assert view["final_tasks"][0]["priority"] == 90
    details = client.get(f"/api/compute/task-details/{task_id}").json()
    assert details["priority"] == 90
    intervention = details["interventions"][-1]
    assert intervention["action"] == "priority_boost" and intervention["request_id"] == request_id


def test_priority_request_idempotent_replay(client):
    create_template(client)
    task_id = make_task(client, "boost-idem-01", priority=50)
    payload = {"actor": "alice", "reason": "幂等提权", "priority": 90, "idempotency_key": "boost-key-0001"}
    first = client.post("/api/compute/intervention-requests", json={"action": "priority_boost", "task_ids": [task_id], **payload})
    again = client.post("/api/compute/intervention-requests", json={"action": "priority_boost", "task_ids": [task_id], **payload})
    assert first.status_code == 201 and again.status_code == 201
    assert first.json()["request"]["id"] == again.json()["request"]["id"]
    changed = {**payload, "priority": 95}
    conflict = client.post("/api/compute/intervention-requests", json={"action": "priority_boost", "task_ids": [task_id], **changed})
    assert conflict.status_code == 409


def test_disabled_policy_allows_direct_boost(client):
    create_template(client)
    task_id = make_task(client, "boost-off-01", priority=50)
    client.put("/api/compute/approval-policies/priority_boost?actor=admin", json={"enabled": False, "priority_delta_threshold": 20, "batch_size_threshold": 2, "ttl_seconds": 3600})
    response = client.post(f"/api/compute/tasks/{task_id}/priority", json={"actor": "alice", "reason": "策略关闭直接提权", "priority": 100})
    assert response.status_code == 200 and response.json()["priority"] == 100
    assert "approval_required" not in response.json()


# ----- 批量重试 -----

def test_batch_retry_at_threshold_requires_approval(client):
    create_template(client)
    first = make_task(client, "batch-retry-1", user="u1")
    second = make_task(client, "batch-retry-2", user="u2")
    fail_task(client, first, worker="w1")
    fail_task(client, second, worker="w2")
    response = client.post(
        "/api/compute/tasks/batch",
        json={"task_ids": [first, second], "operation": "retry", "actor": "alice", "reason": "批量恢复失败任务"},
    )
    assert response.status_code == 200 and response.json()["approval_required"] is True
    request_id = response.json()["request"]["id"]
    approved = client.post(f"/api/compute/intervention-requests/{request_id}/approve", json={"reviewer": "bob"})
    assert approved.status_code == 200
    assert {task["status"] for task in approved.json()["final_tasks"]} == {"queued"}
    assert len(approved.json()["interventions"]) == 2
    assert all(item["request_id"] == request_id for item in approved.json()["interventions"])


# ----- 强制终止 -----

def test_force_terminate_requires_approval(client):
    create_template(client)
    task_id = make_task(client, "kill-01")
    response = client.post(
        f"/api/compute/tasks/{task_id}/force-terminate",
        json={"actor": "alice", "reason": "任务失控需要强制终止", "idempotency_key": "kill-key-00001"},
    )
    assert response.json()["approval_required"] is True
    request_id = response.json()["request"]["id"]
    assert client.get(f"/api/compute/task-details/{task_id}").json()["status"] == "queued"
    approved = client.post(f"/api/compute/intervention-requests/{request_id}/approve", json={"reviewer": "bob"})
    assert approved.json()["status"] == "approved"
    assert client.get(f"/api/compute/task-details/{task_id}").json()["status"] == "cancelled"
    assert approved.json()["interventions"][0]["action"] == "force_terminate"


# ----- 结果撤回 -----

def test_result_withdraw_requires_approval_and_marks_version(client):
    create_template(client)
    task_id = make_task(client, "withdraw-01")
    succeed_task(client, task_id)
    response = client.post(
        f"/api/compute/tasks/{task_id}/withdraw-result",
        json={"actor": "alice", "reason": "结果数据被污染", "idempotency_key": "withdraw-key-01"},
    )
    assert response.json()["approval_required"] is True
    request_id = response.json()["request"]["id"]
    approved = client.post(f"/api/compute/intervention-requests/{request_id}/approve", json={"reviewer": "bob", "reason": "确认撤回"})
    assert approved.status_code == 200
    details = client.get(f"/api/compute/task-details/{task_id}").json()
    assert details["status"] == "result_revoked"
    assert details["current_result_version"] is None
    assert details["results"][0]["withdrawn_at"]
    assert details["results"][0]["withdraw_reason"] == "结果数据被污染"
    assert details["interventions"][-1]["action"] == "result_withdraw"


def test_result_withdraw_direct_when_policy_disabled(client):
    create_template(client)
    task_id = make_task(client, "withdraw-off")
    succeed_task(client, task_id)
    client.put("/api/compute/approval-policies/result_withdraw?actor=admin", json={"enabled": False, "priority_delta_threshold": 0, "batch_size_threshold": 1, "ttl_seconds": 1800})
    response = client.post(f"/api/compute/tasks/{task_id}/withdraw-result", json={"actor": "alice", "reason": "直接撤回"})
    assert response.status_code == 200
    assert client.get(f"/api/compute/task-details/{task_id}").json()["status"] == "result_revoked"


# ----- 驳回、撤销、过期 -----

def test_reject_request_keeps_task_untouched(client):
    create_template(client)
    task_id = make_task(client, "reject-01", priority=50)
    request_id = client.post(
        f"/api/compute/tasks/{task_id}/priority", json={"actor": "alice", "reason": "提权申请", "priority": 95}
    ).json()["request"]["id"]
    rejected = client.post(f"/api/compute/intervention-requests/{request_id}/reject", json={"reviewer": "bob", "reason": "理由不充分"})
    assert rejected.json()["status"] == "rejected"
    assert rejected.json()["decisions"][0]["decision"] == "rejected"
    details = client.get(f"/api/compute/task-details/{task_id}").json()
    assert details["priority"] == 50 and details["interventions"] == []


def test_revoke_by_requester_blocks_later_approval(client):
    create_template(client)
    task_id = make_task(client, "revoke-01", priority=50)
    request_id = client.post(
        f"/api/compute/tasks/{task_id}/priority", json={"actor": "alice", "reason": "提权申请", "priority": 95}
    ).json()["request"]["id"]
    assert client.post(f"/api/compute/intervention-requests/{request_id}/revoke", json={"actor": "carol"}).status_code == 403
    revoked = client.post(f"/api/compute/intervention-requests/{request_id}/revoke", json={"actor": "alice", "reason": "不再需要"})
    assert revoked.json()["status"] == "revoked"
    assert revoked.json()["decisions"][0]["decided_by"] == "alice"
    later = client.post(f"/api/compute/intervention-requests/{request_id}/approve", json={"reviewer": "bob"})
    assert later.json()["status"] == "revoked"
    assert client.get(f"/api/compute/task-details/{task_id}").json()["priority"] == 50


def test_expired_request_cannot_be_approved(client):
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE, "administrator")
    task = service.submit(submit_payload("ttl-expire-01"))
    view = service.create_intervention_request(
        {"action": "priority_boost", "task_ids": [task["id"]], "reason": "临期申请", "priority": 95,
         "idempotency_key": "ttl-key-000001"},
        "alice",
    )
    assert view["status"] == "pending"
    clock.advance(seconds=4000)
    decided = service.approve_request(view["id"], "bob")
    assert decided["status"] == "expired"
    assert decided["decisions"][0]["decision"] == "expired"
    assert service.get_task(task["id"])["priority"] == 50
    listing = service.list_intervention_requests(status="expired")["items"]
    assert [item["id"] for item in listing] == [view["id"]]


# ----- 等待期任务变化：拒绝套用旧决定 -----

def test_approval_rejected_when_task_drifted_then_re_rehearsal_succeeds(client):
    create_template(client)
    task_id = make_task(client, "drift-01", priority=50)
    request_id = client.post(
        f"/api/compute/tasks/{task_id}/priority", json={"actor": "alice", "reason": "大幅提权", "priority": 95}
    ).json()["request"]["id"]
    # 等待期间任务发生变化（版本递增）
    nudged = client.post(f"/api/compute/tasks/{task_id}/priority", json={"actor": "carol", "reason": "小幅微调", "priority": 55})
    assert nudged.status_code == 200 and nudged.json()["priority"] == 55
    stale_view = client.get(f"/api/compute/intervention-requests/{request_id}").json()
    assert stale_view["stale"] is True and stale_view["drift"][0]["task_id"] == task_id
    conflict = client.post(f"/api/compute/intervention-requests/{request_id}/approve", json={"reviewer": "bob"})
    assert conflict.status_code == 409
    assert conflict.json()["error"]["context"]["changed_tasks"][0]["task_id"] == task_id
    # 旧申请未被套用；撤销后基于新版本重新预演、重新申请
    client.post(f"/api/compute/intervention-requests/{request_id}/revoke", json={"actor": "alice"})
    renewed = client.post(
        "/api/compute/intervention-requests",
        json={"action": "priority_boost", "task_ids": [task_id], "actor": "alice", "reason": "大幅提权（重新预演）",
              "priority": 95, "idempotency_key": "drift-key-renewed"},
    ).json()["request"]
    assert renewed["snapshots"][0]["task_version"] == stale_view["snapshots"][0]["task_version"] + 1
    approved = client.post(f"/api/compute/intervention-requests/{renewed['id']}/approve", json={"reviewer": "bob"})
    assert approved.status_code == 200
    assert client.get(f"/api/compute/task-details/{task_id}").json()["priority"] == 95


def test_batch_approval_is_atomic_when_any_task_drifts(client):
    create_template(client)
    first = make_task(client, "batch-drift-1", user="u1")
    second = make_task(client, "batch-drift-2", user="u2")
    request_id = client.post(
        "/api/compute/intervention-requests",
        json={"action": "priority_boost", "task_ids": [first, second], "actor": "alice", "reason": "批量提权",
              "priority": 95, "idempotency_key": "batch-drift-key-1"},
    ).json()["request"]["id"]
    client.post(f"/api/compute/tasks/{second}/priority", json={"actor": "carol", "reason": "微调", "priority": 56})
    conflict = client.post(f"/api/compute/intervention-requests/{request_id}/approve", json={"reviewer": "bob"})
    assert conflict.status_code == 409
    details_first = client.get(f"/api/compute/task-details/{first}").json()
    assert details_first["priority"] == 50 and details_first["interventions"] == []


# ----- 重复决定返回原结果 -----

def test_duplicate_decisions_return_original_outcome(client):
    create_template(client)
    task_id = make_task(client, "dup-decision-01", priority=50)
    request_id = client.post(
        f"/api/compute/tasks/{task_id}/priority", json={"actor": "alice", "reason": "提权", "priority": 95}
    ).json()["request"]["id"]
    first = client.post(f"/api/compute/intervention-requests/{request_id}/approve", json={"reviewer": "bob", "reason": "第一次批准"})
    second = client.post(f"/api/compute/intervention-requests/{request_id}/approve", json={"reviewer": "carol", "reason": "重复批准"})
    assert first.json()["status"] == second.json()["status"] == "approved"
    assert second.json()["decided_by"] == "bob"
    # 已批准后再驳回不能覆盖原决定
    rejected = client.post(f"/api/compute/intervention-requests/{request_id}/reject", json={"reviewer": "carol", "reason": "试图翻案"})
    assert rejected.json()["status"] == "approved"
    details = client.get(f"/api/compute/task-details/{task_id}").json()
    assert len([i for i in details["interventions"] if i["action"] == "priority_boost"]) == 1


# ----- 审计链 -----

def test_audit_chain_links_request_decision_intervention_and_final_state(client):
    create_template(client)
    task_id = make_task(client, "chain-01", priority=50)
    request_id = client.post(
        f"/api/compute/tasks/{task_id}/priority", json={"actor": "alice", "reason": "审计链提权", "priority": 90}
    ).json()["request"]["id"]
    client.post(f"/api/compute/intervention-requests/{request_id}/approve", json={"reviewer": "bob", "reason": "复核通过"})
    chain = client.get(f"/api/compute/intervention-requests/{request_id}").json()
    assert chain["requested_by"] == "alice" and chain["decided_by"] == "bob"
    intervention_id = chain["interventions"][0]["id"]
    # 任务侧能反向找到申请
    details = client.get(f"/api/compute/task-details/{task_id}").json()
    assert details["requests"][0]["id"] == request_id
    linked = next(item for item in details["interventions"] if item["id"] == intervention_id)
    assert linked["request_id"] == request_id
    assert details["status"] == chain["final_tasks"][0]["status"]
    # 列表查询也可按状态过滤
    pending = client.get("/api/compute/intervention-requests?status=approved").json()["items"]
    assert request_id in [item["id"] for item in pending]
