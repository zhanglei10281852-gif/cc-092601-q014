from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.compute.repository import ComputeRepository
from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, PermissionDeniedError, ValidationError
from app.database import get_connection, transaction


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


DEFAULT_APPROVAL_POLICY: dict[str, Any] = {
    "id": 1,
    "priority_threshold": 100,
    "batch_retry_min_size": 2,
    "force_terminate_requires_approval": 1,
    "result_withdraw_requires_approval": 1,
    "request_ttl_seconds": 3600,
    "updated_by": "",
    "created_at": "",
    "updated_at": "",
}

# 审批申请执行时写入干预记录使用的动作名，与直接执行路径保持一致。
EXECUTION_ACTION = {"priority": "priority", "batch_retry": "retry", "force_terminate": "force_terminate", "result_withdraw": "result_withdraw"}


def _mutate_retry(connection: sqlite3.Connection, task: sqlite3.Row, now: str, priority: int | None = None) -> None:
    if task["status"] not in {"failed", "cancelled"}:
        raise ConflictError("只有失败或已取消任务可以人工重试")
    chosen = task["priority"] if priority is None else priority
    connection.execute("UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (chosen, now, now, task["id"]))


def _mutate_priority(connection: sqlite3.Connection, task: sqlite3.Row, now: str, priority: int) -> None:
    if task["status"] not in {"queued", "running"}:
        raise ConflictError("只有排队或运行中的任务可以调整优先级")
    connection.execute("UPDATE compute_tasks SET priority=?,updated_at=?,version=version+1 WHERE id=?", (priority, now, task["id"]))


def _mutate_cancel(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
    if task["status"] not in {"queued", "running"}:
        raise ConflictError("当前任务状态不允许取消")
    status = "cancel_requested" if task["status"] == "running" else "cancelled"
    connection.execute("UPDATE compute_tasks SET status=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?", (status, None if status == "cancel_requested" else now, now, task["id"]))


def _mutate_force_terminate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
    if task["status"] not in {"queued", "running", "cancel_requested"}:
        raise ConflictError("当前任务状态不允许强制终止")
    connection.execute("UPDATE compute_tasks SET status='cancelled',lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?", (now, now, task["id"]))


def _mutate_result_withdraw(connection: sqlite3.Connection, task: sqlite3.Row, now: str, result_version: int, actor: str, reason: str) -> None:
    if task["status"] != "succeeded" or task["current_result_version"] is None:
        raise ConflictError("只有已成功且持有当前结果的任务可以撤回结果")
    if int(task["current_result_version"]) != int(result_version):
        raise ConflictError("撤回的结果版本必须与任务当前结果版本一致")
    row = connection.execute("SELECT withdrawn_at FROM compute_results WHERE task_id=? AND version=?", (task["id"], result_version)).fetchone()
    if row is None:
        raise NotFoundError("结果版本不存在")
    if row["withdrawn_at"]:
        raise ConflictError("结果已经撤回")
    connection.execute("UPDATE compute_results SET withdrawn_at=?,withdrawn_by=?,withdraw_reason=? WHERE task_id=? AND version=?", (now, actor, reason, task["id"], result_version))
    connection.execute("UPDATE compute_tasks SET status='queued',current_result_version=NULL,available_at=?,finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (now, now, task["id"]))


class ComputeOperationsService:
    """管理计算模板、配额、任务租约、结果版本和人工干预。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = ComputeRepository(self.connection)

    def list_templates(self) -> list[dict[str, Any]]:
        return self.repository.active_templates()

    def create_template(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        self._validate_schema(payload["parameter_schema"], payload["default_parameters"])
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.template_by_code(payload["code"]):
                raise ConflictError("参数模板编码已存在")
            return repository.create_template(
                code=payload["code"], name=payload["name"], algorithm=payload["algorithm"],
                parameter_schema=payload["parameter_schema"], defaults=payload["default_parameters"],
                max_runtime_seconds=payload["max_runtime_seconds"], max_attempts=payload["max_attempts"],
                created_by=actor, now=now,
            )

    def set_quota(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            return ComputeRepository(connection).upsert_quota(actor=actor, now=now, **payload)

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            template = repository.template_by_code(payload["template_code"])
            if template is None or not template["active"]:
                raise NotFoundError("参数模板不存在或已经停用")
            parameters = self._validate_parameters(template, payload["parameters"])
            existing = repository.task_by_idempotency(payload["requested_by"], payload["idempotency_key"])
            parameter_digest = digest(parameters)
            if existing is not None:
                if existing["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的计算参数")
                return dict(repository.task_by_id(existing["id"]))
            self._check_quota(repository, payload["requested_by"], now_value)
            return repository.create_task(
                template_id=template["id"], project_code=payload["project_code"],
                requested_by=payload["requested_by"], parameters=parameters,
                parameter_digest=parameter_digest, priority=payload["priority"],
                idempotency_key=payload["idempotency_key"], max_attempts=template["max_attempts"], now=now,
            )

    def list_tasks(self, *, status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=max(1, min(limit, 500)))

    def get_task(self, task_id: int) -> dict[str, Any]:
        row = self.repository.task_by_id(task_id)
        if row is None:
            raise NotFoundError("计算任务不存在")
        result = dict(row)
        result["results"] = self.repository.result_versions(task_id)
        result["interventions"] = self.repository.interventions(task_id)
        return result

    def claim(self, worker_id: str, capabilities: list[str], lease_seconds: int) -> dict[str, Any] | None:
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            candidate = repository.queued_candidate(capabilities, now)
            if candidate is None:
                return None
            cursor = connection.execute(
                "UPDATE compute_tasks SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_expires_at=?,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                (worker_id, lease_until, now, now, candidate["id"]),
            )
            if cursor.rowcount != 1:
                return None
            return dict(repository.task_by_id(candidate["id"]))

    def heartbeat(self, task_id: int, worker_id: str, lease_seconds: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            cursor = connection.execute(
                "UPDATE compute_tasks SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_owner=?",
                (expires, now, task_id, worker_id),
            )
            if cursor.rowcount != 1:
                raise ConflictError("任务未由当前工作者持有")
            return dict(ComputeRepository(connection).task_by_id(task_id))

    def complete(self, task_id: int, worker_id: str, result: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            version = int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM compute_results WHERE task_id=?", (task_id,)).fetchone()[0])
            connection.execute(
                "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (task_id, version, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), digest({"result": result, "metrics": metrics}), worker_id, now),
            )
            connection.execute(
                "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (version, now, now, task_id),
            )
            return dict(repository.task_by_id(task_id))

    def fail(self, task_id: int, worker_id: str, error_code: str, message: str, retryable: bool) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            can_retry = retryable and int(task["attempt_count"]) < int(task["max_attempts"])
            status = "queued" if can_retry else "failed"
            delay = min(300, 2 ** max(0, int(task["attempt_count"]) - 1)) if can_retry else 0
            available = to_storage(now_value + timedelta(seconds=delay))
            connection.execute(
                "UPDATE compute_tasks SET status=?,available_at=?,lease_owner='',lease_expires_at='',last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (status, available, error_code, message[:2000], None if can_retry else now, now, task_id),
            )
            return dict(repository.task_by_id(task_id))

    def cancel(self, task_id: int, actor: str, reason: str, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "cancel", batch_key, _mutate_cancel)

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "retry", batch_key, lambda connection, task, now: _mutate_retry(connection, task, now, priority))

    def set_priority(self, task_id: int, actor: str, reason: str, priority: int, batch_key: str = "") -> dict[str, Any]:
        policy = self.get_policy()
        task = self.repository.task_by_id(task_id)
        if task is None:
            raise NotFoundError("计算任务不存在")
        if priority > int(task["priority"]) and priority >= int(policy["priority_threshold"]):
            request = self._create_request("priority", actor, reason, {"task_ids": [task_id], "priority": priority}, policy)
            return {"approval_required": True, "request": request}
        return self._intervene(task_id, actor, reason, "priority", batch_key, lambda connection, task, now: _mutate_priority(connection, task, now, priority))

    def force_terminate(self, task_id: int, actor: str, reason: str) -> dict[str, Any]:
        policy = self.get_policy()
        if int(policy["force_terminate_requires_approval"]):
            request = self._create_request("force_terminate", actor, reason, {"task_ids": [task_id]}, policy)
            return {"approval_required": True, "request": request}
        return self._intervene(task_id, actor, reason, "force_terminate", "", _mutate_force_terminate)

    def withdraw_result(self, task_id: int, actor: str, reason: str, result_version: int) -> dict[str, Any]:
        policy = self.get_policy()
        if int(policy["result_withdraw_requires_approval"]):
            request = self._create_request("result_withdraw", actor, reason, {"task_ids": [task_id], "result_version": result_version}, policy)
            return {"approval_required": True, "request": request}
        return self._intervene(task_id, actor, reason, "result_withdraw", "", lambda connection, task, now: _mutate_result_withdraw(connection, task, now, result_version, actor, reason))

    def batch_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        policy = self.get_policy()
        operation = payload["operation"]
        task_ids = list(dict.fromkeys(payload["task_ids"]))
        if operation == "retry" and len(task_ids) >= int(policy["batch_retry_min_size"]):
            request = self._create_request("batch_retry", payload["actor"], payload["reason"], {"task_ids": task_ids, "priority": payload.get("priority")}, policy)
            return {"approval_required": True, "request": request}
        if operation == "priority" and payload.get("priority") is not None and int(payload["priority"]) >= int(policy["priority_threshold"]):
            target = int(payload["priority"])
            promoted = False
            for task_id in task_ids:
                task = self.repository.task_by_id(task_id)
                if task is not None and target > int(task["priority"]):
                    promoted = True
                    break
            if promoted:
                request = self._create_request("priority", payload["actor"], payload["reason"], {"task_ids": task_ids, "priority": target}, policy)
                return {"approval_required": True, "request": request}
        batch_key = digest({"actor": payload["actor"], "task_ids": payload["task_ids"], "operation": payload["operation"], "reason": payload["reason"]})
        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for task_id in task_ids:
            try:
                if operation == "cancel":
                    value = self.cancel(task_id, payload["actor"], payload["reason"], batch_key)
                elif operation == "retry":
                    value = self.retry(task_id, payload["actor"], payload["reason"], payload.get("priority"), batch_key)
                else:
                    value = self._intervene(task_id, payload["actor"], payload["reason"], "priority", batch_key, lambda connection, task, now: _mutate_priority(connection, task, now, int(payload["priority"])))
                succeeded.append({"task_id": task_id, "status": value["status"], "version": value["version"]})
            except (ConflictError, NotFoundError) as exc:
                failed.append({"task_id": task_id, "code": exc.code, "message": exc.message})
        return {"batch_key": batch_key, "succeeded": succeeded, "failed": failed}

    def recover_expired(self, actor: str = "recovery-worker") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        recovered: list[int] = []
        exhausted: list[int] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            rows = connection.execute("SELECT * FROM compute_tasks WHERE status='running' AND lease_expires_at<>'' AND lease_expires_at<? ORDER BY id", (now,)).fetchall()
            for task in rows:
                before = dict(task)
                if int(task["attempt_count"]) < int(task["max_attempts"]):
                    status, finished_at = "queued", None
                    recovered.append(int(task["id"]))
                else:
                    status, finished_at = "failed", now
                    exhausted.append(int(task["id"]))
                connection.execute(
                    "UPDATE compute_tasks SET status=?,lease_owner='',lease_expires_at='',available_at=?,last_error_code='lease_expired',last_error_message='工作者租约已过期',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, now, finished_at, now, task["id"]),
                )
                after = dict(repository.task_by_id(task["id"]))
                repository.add_intervention(task_id=task["id"], actor=actor, action="lease_recovery", reason="租约过期自动恢复", before=before, after=after, batch_key="", now=now)
        return {"recovered": recovered, "exhausted": exhausted}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM compute_tasks WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "templates": len(self.repository.active_templates())}

    def _intervene(self, task_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[[sqlite3.Connection, sqlite3.Row, str], None]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            before = dict(task)
            mutation(connection, task, now)
            after = dict(repository.task_by_id(task_id))
            repository.add_intervention(task_id=task_id, actor=actor, action=action, reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            return after

    def get_policy(self) -> dict[str, Any]:
        row = self.repository.policy()
        if row is None:
            return dict(DEFAULT_APPROVAL_POLICY)
        return dict(row)

    def update_policy(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            repository.ensure_policy(actor=actor, now=now)
            if not payload:
                row = repository.policy()
                assert row is not None
                return dict(row)
            return repository.update_policy(fields=payload, actor=actor, now=now)

    def list_requests(self, *, status: str | None = None, applicant: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            repository.expire_requests(now=now)
            rows = repository.list_requests(status=status, applicant=applicant, limit=max(1, min(limit, 500)))
            return [self._request_view(row) for row in rows]

    def get_request(self, request_id: int) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            repository.expire_requests(now=now)
            row = repository.request_by_id(request_id)
            if row is None:
                raise NotFoundError("审批申请不存在")
            return self._request_view(row)

    def decide(self, request_id: int, approver: str, decision: str, reason: str = "") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        stale = False
        view: dict[str, Any] | None = None
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            repository.expire_requests(now=now)
            request = repository.request_by_id(request_id)
            if request is None:
                raise NotFoundError("审批申请不存在")
            if request["status"] in {"approved", "rejected"}:
                return {"request": self._request_view(request), "duplicate": True}
            if request["status"] == "expired":
                raise ConflictError("申请已过期，无法复核")
            if request["status"] == "revoked":
                raise ConflictError("申请已被申请人撤销，无法复核")
            if request["status"] == "stale":
                raise ConflictError("任务快照已过期，需要重新预演后才能复核")
            if approver == request["applicant"]:
                raise PermissionDeniedError("申请人与复核人必须分离")
            if decision == "reject":
                if not reason.strip():
                    raise ValidationError("驳回申请必须填写原因")
                repository.decide_request(request_id=request_id, status="rejected", actor=approver, reason=reason, decided_at=now, execution=None, now=now)
            else:
                refs = json.loads(request["task_refs_json"])
                fresh = True
                for ref in refs:
                    task = repository.task_by_id(int(ref["task_id"]))
                    if task is None or int(task["version"]) != int(ref["version"]):
                        fresh = False
                        break
                if not fresh:
                    repository.update_request_status(request_id=request_id, status="stale", now=now)
                    stale = True
                else:
                    payload = json.loads(request["payload_json"])
                    intervention_ids: list[int] = []
                    tasks_after: list[dict[str, Any]] = []
                    for ref in refs:
                        task_id = int(ref["task_id"])
                        task = repository.task_by_id(task_id)
                        assert task is not None
                        before = dict(task)
                        self._apply_mutation(connection, task, request["action"], payload, now, approver, request["reason"])
                        after = dict(repository.task_by_id(task_id))
                        intervention_id = repository.add_intervention(
                            task_id=task_id, actor=approver, action=EXECUTION_ACTION[request["action"]],
                            reason=request["reason"], before=before, after=after, batch_key=f"approval:{request_id}", now=now,
                        )
                        intervention_ids.append(intervention_id)
                        tasks_after.append({"task_id": task_id, "status": after["status"], "version": after["version"]})
                    execution = {"intervention_ids": intervention_ids, "tasks": tasks_after, "executed_at": now}
                    repository.decide_request(request_id=request_id, status="approved", actor=approver, reason=reason, decided_at=now, execution=execution, now=now)
            view = self._request_view(repository.request_by_id(request_id))
        if stale:
            raise ConflictError("任务在等待期间发生变化，申请已转为快照过期状态，需要重新预演", context={"request_id": request_id, "status": "stale"})
        assert view is not None
        return {"request": view, "duplicate": False}

    def revoke(self, request_id: int, actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            repository.expire_requests(now=now)
            request = repository.request_by_id(request_id)
            if request is None:
                raise NotFoundError("审批申请不存在")
            if actor != request["applicant"]:
                raise PermissionDeniedError("只有申请人可以撤销申请")
            if request["status"] == "revoked":
                return self._request_view(request)
            if request["status"] in {"approved", "rejected"}:
                raise ConflictError("申请已完成复核，无法撤销")
            if request["status"] == "expired":
                raise ConflictError("申请已过期，无需撤销")
            repository.update_request_status(request_id=request_id, status="revoked", now=now)
            return self._request_view(repository.request_by_id(request_id))

    def rehearse(self, request_id: int, actor: str) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            repository.expire_requests(now=now)
            request = repository.request_by_id(request_id)
            if request is None:
                raise NotFoundError("审批申请不存在")
            if actor != request["applicant"]:
                raise PermissionDeniedError("只有申请人可以重新预演申请")
            if request["status"] not in {"pending", "stale"}:
                raise ConflictError("当前状态不允许重新预演")
            policy_row = repository.policy()
            policy = dict(policy_row) if policy_row is not None else dict(DEFAULT_APPROVAL_POLICY)
            payload = json.loads(request["payload_json"])
            task_ids = [int(ref["task_id"]) for ref in json.loads(request["task_refs_json"])]
            self._rehearse(connection, repository, request["action"], payload, task_ids, now, actor, request["reason"])
            refs = self._snapshot(repository, task_ids)
            expires = to_storage(now_value + timedelta(seconds=int(policy["request_ttl_seconds"])))
            repository.refresh_request_rehearsal(request_id=request_id, task_refs=refs, expires_at=expires, now=now)
            return self._request_view(repository.request_by_id(request_id))

    def approval_audit(self, request_id: int) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            repository.expire_requests(now=now)
            row = repository.request_by_id(request_id)
            if row is None:
                raise NotFoundError("审批申请不存在")
            request = self._request_view(row)
            interventions = repository.interventions_by_batch_key(f"approval:{request_id}")
            tasks: list[dict[str, Any]] = []
            for ref in request["task_refs"]:
                task = repository.task_by_id(int(ref["task_id"]))
                if task is not None:
                    tasks.append(dict(task))
            decision: dict[str, Any] | None = None
            if request["status"] in {"approved", "rejected"}:
                decision = {"outcome": request["status"], "actor": request["decision_actor"], "reason": request["decision_reason"], "at": request["decided_at"]}
            elif request["status"] == "revoked":
                decision = {"outcome": "revoked", "actor": request["applicant"], "reason": "", "at": request["updated_at"]}
            elif request["status"] == "expired":
                decision = {"outcome": "expired", "actor": "system", "reason": "申请超过有效期未复核", "at": request["expires_at"]}
            return {"request": request, "decision": decision, "interventions": interventions, "tasks": tasks}

    def _create_request(self, action: str, applicant: str, reason: str, payload: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=int(policy["request_ttl_seconds"])))
        task_ids = [int(task_id) for task_id in payload["task_ids"]]
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            self._rehearse(connection, repository, action, payload, task_ids, now, applicant, reason)
            refs = self._snapshot(repository, task_ids)
            row = repository.create_request(action=action, applicant=applicant, reason=reason, payload=payload, task_refs=refs, expires_at=expires, now=now)
            return self._request_view(row)

    def _rehearse(self, connection: sqlite3.Connection, repository: ComputeRepository, action: str, payload: dict[str, Any], task_ids: list[int], now: str, actor: str, reason: str) -> None:
        """在保存点内试跑真实变更逻辑验证可行性，随后回滚，不落任何数据。"""
        connection.execute("SAVEPOINT approval_rehearsal")
        try:
            for task_id in task_ids:
                task = repository.task_by_id(task_id)
                if task is None:
                    raise NotFoundError(f"计算任务 {task_id} 不存在")
                self._apply_mutation(connection, task, action, payload, now, actor, reason)
        finally:
            connection.execute("ROLLBACK TO approval_rehearsal")
            connection.execute("RELEASE approval_rehearsal")

    @staticmethod
    def _snapshot(repository: ComputeRepository, task_ids: list[int]) -> list[dict[str, Any]]:
        refs: list[dict[str, Any]] = []
        for task_id in task_ids:
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError(f"计算任务 {task_id} 不存在")
            refs.append({"task_id": task_id, "version": int(task["version"])})
        return refs

    @staticmethod
    def _apply_mutation(connection: sqlite3.Connection, task: sqlite3.Row, action: str, payload: dict[str, Any], now: str, actor: str, reason: str) -> None:
        if action == "priority":
            _mutate_priority(connection, task, now, int(payload["priority"]))
        elif action in {"retry", "batch_retry"}:
            _mutate_retry(connection, task, now, payload.get("priority"))
        elif action == "cancel":
            _mutate_cancel(connection, task, now)
        elif action == "force_terminate":
            _mutate_force_terminate(connection, task, now)
        elif action == "result_withdraw":
            _mutate_result_withdraw(connection, task, now, int(payload["result_version"]), actor, reason)
        else:
            raise ValidationError(f"不支持的审批操作类型：{action}")

    @staticmethod
    def _request_view(row: sqlite3.Row) -> dict[str, Any]:
        view = dict(row)
        view["payload"] = json.loads(view.pop("payload_json"))
        view["task_refs"] = json.loads(view.pop("task_refs_json"))
        view["execution"] = json.loads(view.pop("execution_json") or "{}")
        return view

    def _check_quota(self, repository: ComputeRepository, requested_by: str, now: datetime) -> None:
        quota = repository.quota("user", requested_by)
        if quota is None:
            return
        states = repository.count_user_states(requested_by)
        if states.get("queued", 0) >= int(quota["max_queued"]):
            raise ConflictError("用户排队任务配额已用尽")
        if states.get("running", 0) >= int(quota["max_running"]):
            raise ConflictError("用户运行任务配额已用尽")
        day_start = to_storage(now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0))
        if repository.count_user_submissions_since(requested_by, day_start) >= int(quota["daily_submissions"]):
            raise ConflictError("用户当日提交配额已用尽")

    @staticmethod
    def _validate_schema(schema: dict[str, dict[str, Any]], defaults: dict[str, Any]) -> None:
        if not schema:
            raise ValidationError("参数模板至少包含一个参数")
        allowed = {"integer", "number", "string", "boolean"}
        for name, rule in schema.items():
            if not name or not isinstance(rule, dict) or rule.get("type") not in allowed:
                raise ValidationError(f"参数 {name or '<empty>'} 的规则不合法")
        if set(defaults) - set(schema):
            raise ValidationError("默认值包含未声明参数")

    def _validate_parameters(self, template: sqlite3.Row, supplied: dict[str, Any]) -> dict[str, Any]:
        schema = json.loads(template["parameter_schema_json"])
        values = {**json.loads(template["default_parameters_json"]), **supplied}
        unknown = set(values) - set(schema)
        if unknown:
            raise ValidationError("包含模板未声明的参数", context={"parameters": sorted(unknown)})
        normalized: dict[str, Any] = {}
        for name, rule in schema.items():
            if name not in values:
                if rule.get("required"):
                    raise ValidationError(f"缺少必填参数：{name}")
                continue
            value = values[name]
            kind = rule["type"]
            valid = {"integer": isinstance(value, int) and not isinstance(value, bool), "number": isinstance(value, (int, float)) and not isinstance(value, bool), "string": isinstance(value, str), "boolean": isinstance(value, bool)}[kind]
            if not valid:
                raise ValidationError(f"参数 {name} 类型不正确")
            if rule.get("minimum") is not None and value < rule["minimum"]:
                raise ValidationError(f"参数 {name} 小于允许的最小值")
            if rule.get("maximum") is not None and value > rule["maximum"]:
                raise ValidationError(f"参数 {name} 大于允许的最大值")
            if rule.get("choices") and value not in rule["choices"]:
                raise ValidationError(f"参数 {name} 不在允许的选项中")
            normalized[name] = value
        return normalized
