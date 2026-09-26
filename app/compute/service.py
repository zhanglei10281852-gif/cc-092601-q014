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


HIGH_RISK_ACTIONS = ("priority_boost", "batch_retry", "force_terminate", "result_withdraw")
# 各动作申请必须满足的任务当前状态
ACTION_REQUIRED_STATUSES = {
    "priority_boost": {"queued", "running"},
    "batch_retry": {"failed", "cancelled"},
    "force_terminate": {"queued", "running", "cancel_requested"},
    "result_withdraw": {"succeeded"},
}


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


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
        result["requests"] = self.repository.requests_for_task(task_id)
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
        return self._intervene(task_id, actor, reason, "cancel", batch_key, self._cancel_mutation)

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "", request_id: int | None = None) -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "retry", batch_key, self._retry_mutation(priority), request_id=request_id)

    def set_priority(self, task_id: int, actor: str, reason: str, priority: int, batch_key: str = "", request_id: int | None = None, idempotency_key: str | None = None) -> dict[str, Any]:
        # 超过策略阈值的优先级提升属于高风险干预，先生成带版本快照的申请
        if request_id is None:
            gated = self._priority_approval_request(task_id, actor, reason, priority, idempotency_key)
            if gated is not None:
                return gated
        return self._intervene(task_id, actor, reason, "priority", batch_key, self._priority_mutation(priority), request_id=request_id)

    def _priority_approval_request(self, task_id: int, actor: str, reason: str, priority: int, idempotency_key: str | None) -> dict[str, Any] | None:
        policy = self.repository.approval_policy("priority_boost")
        if policy is None or int(policy["enabled"]) == 0:
            return None
        task = self.repository.task_by_id(task_id)
        if task is None:
            return None
        if int(priority) - int(task["priority"]) < int(policy["priority_delta_threshold"]):
            return None
        key = idempotency_key or digest({"actor": actor, "action": "priority_boost", "task_ids": [task_id], "priority": priority, "reason": reason})
        view = self.create_intervention_request(
            {"action": "priority_boost", "task_ids": [task_id], "reason": reason, "priority": int(priority), "idempotency_key": key},
            actor,
        )
        return {"approval_required": True, "request": view}

    def force_terminate(self, task_id: int, actor: str, reason: str, batch_key: str = "", request_id: int | None = None) -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "force_terminate", batch_key, self._force_terminate_mutation, request_id=request_id)

    def withdraw_result(self, task_id: int, actor: str, reason: str, batch_key: str = "", request_id: int | None = None) -> dict[str, Any]:
        withdrawn_at = to_storage(self.clock.now())
        return self._intervene(task_id, actor, reason, "result_withdraw", batch_key, self._withdraw_mutation(actor, reason, withdrawn_at), request_id=request_id)

    @staticmethod
    def _retry_mutation(priority: int | None) -> Callable[[sqlite3.Connection, sqlite3.Row, str], None]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消任务可以人工重试")
            chosen = task["priority"] if priority is None else priority
            connection.execute("UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (chosen, now, now, task["id"]))
        return mutate

    @staticmethod
    def _priority_mutation(priority: int) -> Callable[[sqlite3.Connection, sqlite3.Row, str], None]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"queued", "running"}:
                raise ConflictError("只有排队或运行中的任务可以调整优先级")
            connection.execute("UPDATE compute_tasks SET priority=?,updated_at=?,version=version+1 WHERE id=?", (priority, now, task["id"]))
        return mutate

    @staticmethod
    def _force_terminate_mutation(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
        if task["status"] not in {"queued", "running", "cancel_requested"}:
            raise ConflictError("只有排队、运行中或待取消的任务可以强制终止")
        connection.execute(
            "UPDATE compute_tasks SET status='cancelled',lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
            (now, now, task["id"]),
        )

    @staticmethod
    def _withdraw_mutation(actor: str, reason: str, now: str) -> Callable[[sqlite3.Connection, sqlite3.Row, str], None]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now_value: str) -> None:
            if task["status"] != "succeeded" or task["current_result_version"] is None:
                raise ConflictError("只有已成功且存在结果版本的任务可以撤回结果")
            connection.execute(
                "UPDATE compute_results SET withdrawn_at=?,withdraw_reason=? WHERE task_id=? AND version=?",
                (now, reason, task["id"], task["current_result_version"]),
            )
            connection.execute(
                "UPDATE compute_tasks SET status='result_revoked',current_result_version=NULL,updated_at=?,version=version+1 WHERE id=?",
                (now_value, task["id"]),
            )
        return mutate

    def batch_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        task_ids = list(dict.fromkeys(payload["task_ids"]))
        # 批量重试达到策略阈值时，先生成带版本快照的审批申请
        if payload["operation"] == "retry":
            policy = self.repository.approval_policy("batch_retry")
            if policy is not None and int(policy["enabled"]) == 1 and len(task_ids) >= int(policy["batch_size_threshold"]):
                view = self.create_intervention_request(
                    {"action": "batch_retry", "task_ids": task_ids, "reason": payload["reason"],
                     "priority": payload.get("priority"),
                     "idempotency_key": digest({"actor": payload["actor"], "task_ids": task_ids, "operation": "retry", "reason": payload["reason"]})},
                    payload["actor"],
                )
                return {"approval_required": True, "batch_key": None, "request": view, "succeeded": [], "failed": []}
        # 批量调优先级时，只要有任务的提升幅度超过阈值，整批转入审批
        if payload["operation"] == "priority":
            gated = self._batch_priority_request(task_ids, payload)
            if gated is not None:
                return gated
        batch_key = digest({"actor": payload["actor"], "task_ids": payload["task_ids"], "operation": payload["operation"], "reason": payload["reason"]})
        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for task_id in task_ids:
            try:
                if payload["operation"] == "cancel":
                    value = self.cancel(task_id, payload["actor"], payload["reason"], batch_key)
                elif payload["operation"] == "retry":
                    value = self.retry(task_id, payload["actor"], payload["reason"], payload.get("priority"), batch_key)
                else:
                    value = self.set_priority(task_id, payload["actor"], payload["reason"], int(payload["priority"]), batch_key)
                succeeded.append({"task_id": task_id, "status": value["status"], "version": value["version"]})
            except (ConflictError, NotFoundError) as exc:
                failed.append({"task_id": task_id, "code": exc.code, "message": exc.message})
        return {"batch_key": batch_key, "succeeded": succeeded, "failed": failed}

    def _batch_priority_request(self, task_ids: list[int], payload: dict[str, Any]) -> dict[str, Any] | None:
        policy = self.repository.approval_policy("priority_boost")
        if policy is None or int(policy["enabled"]) == 0:
            return None
        threshold = int(policy["priority_delta_threshold"])
        crossing = False
        for task_id in task_ids:
            task = self.repository.task_by_id(task_id)
            if task is not None and int(payload["priority"]) - int(task["priority"]) >= threshold:
                crossing = True
                break
        if not crossing:
            return None
        view = self.create_intervention_request(
            {"action": "priority_boost", "task_ids": task_ids, "reason": payload["reason"],
             "priority": int(payload["priority"]),
             "idempotency_key": digest({"actor": payload["actor"], "task_ids": task_ids, "operation": "priority", "priority_value": int(payload["priority"]), "reason": payload["reason"]})},
            payload["actor"],
        )
        return {"approval_required": True, "batch_key": None, "request": view, "succeeded": [], "failed": []}

    # ----- 高风险干预：审批策略 -----

    def list_approval_policies(self) -> list[dict[str, Any]]:
        return self.repository.list_approval_policies()

    def update_approval_policy(self, action: str, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        if action not in HIGH_RISK_ACTIONS:
            raise ValidationError("不支持的高风险干预类型")
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            try:
                return ComputeRepository(connection).update_approval_policy(
                    action=action, enabled=payload["enabled"],
                    priority_delta_threshold=payload["priority_delta_threshold"],
                    batch_size_threshold=payload["batch_size_threshold"],
                    ttl_seconds=payload["ttl_seconds"], actor=actor, now=now,
                )
            except KeyError:
                raise NotFoundError("审批策略不存在") from None

    def request_intervention(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        """显式创建高风险干预申请（强制终止、结果撤回、大幅提权、批量重试）。"""
        return {"approval_required": True, "request": self.create_intervention_request(payload, actor)}

    def force_terminate_request_or_execute(self, task_id: int, actor: str, reason: str) -> dict[str, Any]:
        policy = self.repository.approval_policy("force_terminate")
        if policy is not None and int(policy["enabled"]) == 1:
            view = self.create_intervention_request(
                {"action": "force_terminate", "task_ids": [task_id], "reason": reason,
                 "idempotency_key": digest({"actor": actor, "action": "force_terminate", "task_ids": [task_id], "reason": reason})},
                actor,
            )
            return {"approval_required": True, "request": view}
        return self.force_terminate(task_id, actor, reason)

    def withdraw_result_request_or_execute(self, task_id: int, actor: str, reason: str) -> dict[str, Any]:
        policy = self.repository.approval_policy("result_withdraw")
        if policy is not None and int(policy["enabled"]) == 1:
            view = self.create_intervention_request(
                {"action": "result_withdraw", "task_ids": [task_id], "reason": reason,
                 "idempotency_key": digest({"actor": actor, "action": "result_withdraw", "task_ids": [task_id], "reason": reason})},
                actor,
            )
            return {"approval_required": True, "request": view}
        return self.withdraw_result(task_id, actor, reason)

    def create_intervention_request(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        action = payload["action"]
        if action not in HIGH_RISK_ACTIONS:
            raise ValidationError("不支持的高风险干预类型")
        task_ids = list(dict.fromkeys(payload["task_ids"]))
        if not task_ids:
            raise ValidationError("至少选择一个任务")
        if action == "priority_boost" and payload.get("priority") is None:
            raise ValidationError("优先级提升必须提供目标优先级")
        now_value = self.clock.now()
        now = to_storage(now_value)
        requester_payload = {"action": action, "task_ids": task_ids, "reason": payload["reason"], "priority": payload.get("priority")}
        payload_digest = digest(requester_payload)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            existing = repository.request_by_idempotency(actor, payload["idempotency_key"])
            if existing is not None:
                if digest(json.loads(existing["payload_json"])) != payload_digest:
                    raise ConflictError("同一幂等键不能用于不同的干预申请")
                return self._request_view(repository, existing, now)
            policy = repository.approval_policy(action)
            ttl = int(policy["ttl_seconds"]) if policy is not None else 3600
            expires_at = to_storage(now_value + timedelta(seconds=ttl))
            snapshots = self._load_snapshots(repository, task_ids, action, payload)
            request_id = repository.create_request(
                action=action, requested_by=actor, reason=payload["reason"],
                payload=requester_payload, snapshot_digest=digest(snapshots),
                idempotency_key=payload["idempotency_key"], expires_at=expires_at, now=now,
            )
            for snap in snapshots:
                repository.add_request_task(
                    request_id=request_id, task_id=snap["task_id"], task_version=snap["task_version"],
                    task_status=snap["task_status"], result_version=snap["result_version"],
                    priority=snap["priority"], now=now,
                )
            return self._request_view(repository, repository.request_by_id(request_id), now)

    def list_intervention_requests(self, *, status: str | None = None, action: str | None = None, limit: int = 100) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            repository.expire_requests(now)
            rows = repository.list_requests(status=status, action=action, limit=max(1, min(limit, 500)))
            return {"items": [self._request_view(repository, row, now) for row in rows]}

    def get_intervention_request(self, request_id: int) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            repository.expire_requests(now)
            row = repository.request_by_id(request_id)
            if row is None:
                raise NotFoundError("干预申请不存在")
            return self._request_view(repository, row, now)

    def approve_request(self, request_id: int, reviewer: str, reason: str = "") -> dict[str, Any]:
        return self._decide_request(request_id, reviewer, "approved", reason)

    def reject_request(self, request_id: int, reviewer: str, reason: str) -> dict[str, Any]:
        return self._decide_request(request_id, reviewer, "rejected", reason)

    def revoke_request(self, request_id: int, actor: str, reason: str = "") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            repository.expire_requests(now)
            row = repository.request_by_id(request_id)
            if row is None:
                raise NotFoundError("干预申请不存在")
            if row["status"] != "pending":
                # 已终结的申请：重复撤销直接返回原状态
                return self._request_view(repository, row, now)
            if row["requested_by"] != actor:
                raise PermissionDeniedError("只有申请人可以撤销自己的干预申请")
            connection.execute(
                "UPDATE compute_intervention_requests SET status='revoked',revoked_by=?,revoked_at=?,decided_by=?,decided_at=?,updated_at=? WHERE id=?",
                (actor, now, actor, now, now, request_id),
            )
            repository.add_decision(request_id=request_id, decision="revoked", decided_by=actor, reason=reason or "申请人撤销", now=now)
            return self._request_view(repository, repository.request_by_id(request_id), now)

    def _decide_request(self, request_id: int, reviewer: str, decision: str, reason: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            repository.expire_requests(now)
            row = repository.request_by_id(request_id)
            if row is None:
                raise NotFoundError("干预申请不存在")
            if row["status"] != "pending":
                # 重复决定必须返回原结果，不能套用或覆盖旧决定
                return self._request_view(repository, row, now)
            if row["requested_by"] == reviewer:
                raise PermissionDeniedError("申请人与复核人必须分离")
            if decision == "rejected":
                repository.mark_request(request_id, status="rejected", decided_by=reviewer, decided_at=now, decision_reason=reason)
                repository.add_decision(request_id=request_id, decision="rejected", decided_by=reviewer, reason=reason, now=now)
                return self._request_view(repository, repository.request_by_id(request_id), now)

            # 批准前校验：任务版本快照必须与当前一致，否则拒绝套用旧决定
            snapshots = repository.request_tasks(request_id)
            drift = self._detect_drift(repository, snapshots)
            if drift:
                raise ConflictError("申请等待期间任务已发生变化，请重新预演后再次申请", context={"changed_tasks": drift})
            payload = json.loads(row["payload_json"])
            mutation = self._mutation_for(row["action"], payload, reviewer, row["reason"])
            batch_key = digest({"request_id": request_id, "action": row["action"]})
            for snap in snapshots:
                self._apply_intervention(
                    connection, repository, snap["task_id"], reviewer, row["reason"], row["action"],
                    batch_key, mutation, now, request_id,
                )
            repository.mark_request(request_id, status="approved", decided_by=reviewer, decided_at=now, decision_reason=reason, batch_key=batch_key, executed_at=now)
            repository.add_decision(request_id=request_id, decision="approved", decided_by=reviewer, reason=reason, now=now)
            return self._request_view(repository, repository.request_by_id(request_id), now)

    # ----- 审批工作流辅助 -----

    def _load_snapshots(self, repository: ComputeRepository, task_ids: list[int], action: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
        required = ACTION_REQUIRED_STATUSES[action]
        snapshots: list[dict[str, Any]] = []
        invalid: list[dict[str, Any]] = []
        for task_id in task_ids:
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError(f"计算任务 {task_id} 不存在")
            if task["status"] not in required:
                invalid.append({"task_id": task_id, "status": task["status"], "required": sorted(required)})
            snapshots.append({
                "task_id": task_id, "task_version": int(task["version"]), "task_status": task["status"],
                "result_version": task["current_result_version"], "priority": int(task["priority"]),
            })
        if invalid:
            raise ConflictError("部分任务当前状态不允许该干预", context={"invalid_tasks": invalid})
        if action == "priority_boost":
            target = int(payload["priority"])
            if all(target <= snap["priority"] for snap in snapshots):
                raise ValidationError("优先级提升的目标值必须高于至少一个任务的当前优先级")
        return snapshots

    @staticmethod
    def _detect_drift(repository: ComputeRepository, snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
        changed: list[dict[str, Any]] = []
        for snap in snapshots:
            task = repository.task_by_id(snap["task_id"])
            if task is None:
                changed.append({"task_id": snap["task_id"], "reason": "task_missing"})
                continue
            current = {"task_version": int(task["version"]), "task_status": task["status"],
                       "result_version": task["current_result_version"], "priority": int(task["priority"])}
            expected = {key: snap[key] for key in current}
            if current != expected:
                changed.append({"task_id": snap["task_id"], "snapshot": expected, "current": current})
        return changed

    def _request_view(self, repository: ComputeRepository, row: sqlite3.Row, now: str) -> dict[str, Any]:
        request_id = int(row["id"])
        snapshots = repository.request_tasks(request_id)
        decisions = repository.request_decisions(request_id)
        interventions = [dict(item) for item in repository.interventions_by_request(request_id)]
        final_tasks: list[dict[str, Any]] = []
        drift: list[dict[str, Any]] = []
        for snap in snapshots:
            task = repository.task_by_id(snap["task_id"])
            if task is not None:
                task_view = dict(task)
                final_tasks.append({"task_id": task_view["id"], "status": task_view["status"], "priority": task_view["priority"], "version": task_view["version"], "current_result_version": task_view["current_result_version"]})
        if row["status"] == "pending":
            drift = self._detect_drift(repository, snapshots)
        return {
            "id": request_id,
            "action": row["action"],
            "requested_by": row["requested_by"],
            "reason": row["reason"],
            "payload": json.loads(row["payload_json"]),
            "status": row["status"],
            "expires_at": row["expires_at"],
            "created_at": row["created_at"],
            "decided_by": row["decided_by"],
            "decided_at": row["decided_at"],
            "decision_reason": row["decision_reason"],
            "revoked_by": row["revoked_by"],
            "revoked_at": row["revoked_at"],
            "executed_at": row["executed_at"],
            "batch_key": row["batch_key"],
            "snapshots": snapshots,
            "decisions": decisions,
            "interventions": interventions,
            "final_tasks": final_tasks,
            "drift": drift,
            "stale": bool(drift),
        }

    def recover_expired(self, actor: str = "recovery-worker") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        recovered: list[int] = []
        exhausted: list[int] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            expired_requests = repository.expire_requests(now)
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
        return {"recovered": recovered, "exhausted": exhausted, "expired_requests": expired_requests}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM compute_tasks WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "templates": len(self.repository.active_templates())}

    def _intervene(self, task_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[[sqlite3.Connection, sqlite3.Row, str], None], request_id: int | None = None) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            return self._apply_intervention(connection, repository, task_id, actor, reason, action, batch_key, mutation, now, request_id)

    def _apply_intervention(self, connection: sqlite3.Connection, repository: ComputeRepository, task_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[[sqlite3.Connection, sqlite3.Row, str], None], now: str, request_id: int | None) -> dict[str, Any]:
        task = repository.task_by_id(task_id)
        if task is None:
            raise NotFoundError("计算任务不存在")
        before = dict(task)
        mutation(connection, task, now)
        after = dict(repository.task_by_id(task_id))
        repository.add_intervention(task_id=task_id, actor=actor, action=action, reason=reason, before=before, after=after, batch_key=batch_key, now=now, request_id=request_id)
        return after

    def _mutation_for(self, action: str, payload: dict[str, Any], actor: str, reason: str) -> Callable[[sqlite3.Connection, sqlite3.Row, str], None]:
        if action == "priority_boost":
            return self._priority_mutation(int(payload["priority"]))
        if action == "batch_retry":
            return self._retry_mutation(payload.get("priority"))
        if action == "force_terminate":
            return self._force_terminate_mutation
        if action == "result_withdraw":
            return self._withdraw_mutation(actor, reason, to_storage(self.clock.now()))
        raise ValidationError("不支持的高风险干预类型")

    @staticmethod
    def _cancel_mutation(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
        if task["status"] not in {"queued", "running"}:
            raise ConflictError("当前任务状态不允许取消")
        status = "cancel_requested" if task["status"] == "running" else "cancelled"
        connection.execute("UPDATE compute_tasks SET status=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?", (status, None if status == "cancel_requested" else now, now, task["id"]))

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
