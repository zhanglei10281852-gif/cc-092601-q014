from __future__ import annotations

import json
import sqlite3
from typing import Any, Iterable


class ComputeRepository:
    """封装计算任务运营领域的 SQLite 读写。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def template_by_code(self, code: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_templates WHERE code=?", (code,)).fetchone()

    def template_by_id(self, template_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_templates WHERE id=?", (template_id,)).fetchone()

    def active_templates(self) -> list[dict[str, Any]]:
        rows = self.connection.execute("SELECT * FROM compute_templates WHERE active=1 ORDER BY code,version").fetchall()
        return [dict(row) for row in rows]

    def create_template(self, *, code: str, name: str, algorithm: str, parameter_schema: dict[str, Any], defaults: dict[str, Any], max_runtime_seconds: int, max_attempts: int, created_by: str, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_templates(code,name,algorithm,version,parameter_schema_json,default_parameters_json,max_runtime_seconds,max_attempts,active,created_by,created_at,updated_at) VALUES(?,?,?,1,?,?,?,?,1,?,?,?)",
            (code, name, algorithm, json.dumps(parameter_schema, ensure_ascii=False, sort_keys=True), json.dumps(defaults, ensure_ascii=False, sort_keys=True), max_runtime_seconds, max_attempts, created_by, now, now),
        )
        return dict(self.template_by_id(cursor.lastrowid))

    def quota(self, subject_type: str, subject_key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_quotas WHERE subject_type=? AND subject_key=?", (subject_type, subject_key)).fetchone()

    def upsert_quota(self, *, subject_type: str, subject_key: str, max_queued: int, max_running: int, daily_submissions: int, actor: str, now: str) -> dict[str, Any]:
        self.connection.execute(
            "INSERT INTO compute_quotas(subject_type,subject_key,max_queued,max_running,daily_submissions,updated_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(subject_type,subject_key) DO UPDATE SET max_queued=excluded.max_queued,max_running=excluded.max_running,daily_submissions=excluded.daily_submissions,updated_by=excluded.updated_by,updated_at=excluded.updated_at",
            (subject_type, subject_key, max_queued, max_running, daily_submissions, actor, now, now),
        )
        return dict(self.quota(subject_type, subject_key))

    def count_user_states(self, requested_by: str) -> dict[str, int]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks WHERE requested_by=? GROUP BY status", (requested_by,)).fetchall()
        return {str(row["status"]): int(row["amount"]) for row in rows}

    def count_user_submissions_since(self, requested_by: str, since: str) -> int:
        return int(self.connection.execute("SELECT COUNT(*) FROM compute_tasks WHERE requested_by=? AND created_at>=?", (requested_by, since)).fetchone()[0])

    def task_by_id(self, task_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT t.*,tpl.code AS template_code,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id WHERE t.id=?", (task_id,)).fetchone()

    def task_by_idempotency(self, requested_by: str, key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_tasks WHERE requested_by=? AND idempotency_key=?", (requested_by, key)).fetchone()

    def create_task(self, *, template_id: int, project_code: str, requested_by: str, parameters: dict[str, Any], parameter_digest: str, priority: int, idempotency_key: str, max_attempts: int, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_tasks(template_id,project_code,requested_by,parameters_json,parameter_digest,priority,idempotency_key,status,attempt_count,max_attempts,available_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'queued',0,?,?,?,?)",
            (template_id, project_code, requested_by, json.dumps(parameters, ensure_ascii=False, sort_keys=True), parameter_digest, priority, idempotency_key, max_attempts, now, now, now),
        )
        return dict(self.task_by_id(cursor.lastrowid))

    def queued_candidate(self, capabilities: Iterable[str], now: str) -> sqlite3.Row | None:
        capability_list = sorted(set(capabilities))
        params: list[Any] = [now]
        condition = ""
        if capability_list:
            placeholders = ",".join("?" for _ in capability_list)
            condition = f" AND tpl.algorithm IN ({placeholders})"
            params.extend(capability_list)
        return self.connection.execute(
            "SELECT t.*,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id WHERE t.status='queued' AND t.available_at<=?" + condition + " ORDER BY t.priority DESC,t.created_at ASC,t.id ASC LIMIT 1",
            params,
        ).fetchone()

    def result_versions(self, task_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_results WHERE task_id=? ORDER BY version", (task_id,)).fetchall()]

    def interventions(self, task_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_interventions WHERE task_id=? ORDER BY id", (task_id,)).fetchall()]

    def add_intervention(self, *, task_id: int, actor: str, action: str, reason: str, before: dict[str, Any], after: dict[str, Any], batch_key: str, now: str, request_id: int | None = None) -> None:
        self.connection.execute(
            "INSERT INTO compute_interventions(task_id,actor,action,reason,before_json,after_json,batch_key,request_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (task_id, actor, action, reason, json.dumps(before, ensure_ascii=False, sort_keys=True), json.dumps(after, ensure_ascii=False, sort_keys=True), batch_key, request_id, now),
        )

    # ----- 审批策略 -----

    def list_approval_policies(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_approval_policies ORDER BY id").fetchall()]

    def approval_policy(self, action: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_approval_policies WHERE action=?", (action,)).fetchone()

    def update_approval_policy(self, *, action: str, enabled: bool, priority_delta_threshold: int, batch_size_threshold: int, ttl_seconds: int, actor: str, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "UPDATE compute_approval_policies SET enabled=?,priority_delta_threshold=?,batch_size_threshold=?,ttl_seconds=?,updated_by=?,updated_at=? WHERE action=?",
            (1 if enabled else 0, priority_delta_threshold, batch_size_threshold, ttl_seconds, actor, now, action),
        )
        if cursor.rowcount != 1:
            raise KeyError(action)
        return dict(self.approval_policy(action))

    # ----- 高风险干预申请 -----

    def create_request(self, *, action: str, requested_by: str, reason: str, payload: dict[str, Any], snapshot_digest: str, idempotency_key: str, expires_at: str, now: str) -> int:
        cursor = self.connection.execute(
            "INSERT INTO compute_intervention_requests(action,requested_by,reason,payload_json,snapshot_digest,idempotency_key,status,expires_at,created_at,updated_at) VALUES(?,?,?,?,?,?,'pending',?,?,?)",
            (action, requested_by, reason, json.dumps(payload, ensure_ascii=False, sort_keys=True), snapshot_digest, idempotency_key, expires_at, now, now),
        )
        return int(cursor.lastrowid)

    def add_request_task(self, *, request_id: int, task_id: int, task_version: int, task_status: str, result_version: int | None, priority: int, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_intervention_request_tasks(request_id,task_id,task_version,task_status,result_version,priority,created_at) VALUES(?,?,?,?,?,?,?)",
            (request_id, task_id, task_version, task_status, result_version, priority, now),
        )

    def request_by_id(self, request_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_intervention_requests WHERE id=?", (request_id,)).fetchone()

    def request_by_idempotency(self, requested_by: str, key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_intervention_requests WHERE requested_by=? AND idempotency_key=?", (requested_by, key)).fetchone()

    def request_tasks(self, request_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_intervention_request_tasks WHERE request_id=? ORDER BY id", (request_id,)).fetchall()]

    def requests_for_task(self, task_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT r.* FROM compute_intervention_requests r JOIN compute_intervention_request_tasks rt ON rt.request_id=r.id WHERE rt.task_id=? ORDER BY r.id",
            (task_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def request_decisions(self, request_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_intervention_decisions WHERE request_id=? ORDER BY id", (request_id,)).fetchall()]

    def list_requests(self, *, status: str | None, action: str | None, limit: int) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if status:
            clauses.append("status=?")
            values.append(status)
        if action:
            clauses.append("action=?")
            values.append(action)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        values.append(limit)
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM compute_intervention_requests" + where + " ORDER BY id DESC LIMIT ?", values,
        ).fetchall()]

    def expire_requests(self, now: str) -> list[int]:
        rows = self.connection.execute(
            "SELECT id FROM compute_intervention_requests WHERE status='pending' AND expires_at<=? ORDER BY id",
            (now,),
        ).fetchall()
        expired = [int(row["id"]) for row in rows]
        for request_id in expired:
            self.connection.execute(
                "UPDATE compute_intervention_requests SET status='expired',decided_by='system:ttl',decided_at=?,updated_at=? WHERE id=? AND status='pending'",
                (now, now, request_id),
            )
            self.connection.execute(
                "INSERT OR IGNORE INTO compute_intervention_decisions(request_id,decision,decided_by,reason,created_at) VALUES(?,'expired','system:ttl','申请已超过有效期',?)",
                (request_id, now),
            )
        return expired

    def mark_request(self, request_id: int, *, status: str, decided_by: str, decided_at: str, decision_reason: str = "", batch_key: str = "", executed_at: str | None = None) -> None:
        self.connection.execute(
            "UPDATE compute_intervention_requests SET status=?,decided_by=?,decided_at=?,decision_reason=?,batch_key=COALESCE(NULLIF(?,''),batch_key),executed_at=COALESCE(?,executed_at),updated_at=? WHERE id=?",
            (status, decided_by, decided_at, decision_reason, batch_key, executed_at, decided_at, request_id),
        )

    def add_decision(self, *, request_id: int, decision: str, decided_by: str, reason: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_intervention_decisions(request_id,decision,decided_by,reason,created_at) VALUES(?,?,?,?,?)",
            (request_id, decision, decided_by, reason, now),
        )

    def interventions_by_request(self, request_id: int) -> list[sqlite3.Row]:
        return self.connection.execute("SELECT * FROM compute_interventions WHERE request_id=? ORDER BY id", (request_id,)).fetchall()

    def list_tasks(self, *, status: str | None, project_code: str | None, requested_by: str | None, limit: int) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if status:
            clauses.append("t.status=?")
            values.append(status)
        if project_code:
            clauses.append("t.project_code=?")
            values.append(project_code)
        if requested_by:
            clauses.append("t.requested_by=?")
            values.append(requested_by)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        values.append(limit)
        rows = self.connection.execute(
            "SELECT t.*,tpl.code AS template_code,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id" + where + " ORDER BY t.priority DESC,t.created_at DESC,t.id DESC LIMIT ?",
            values,
        ).fetchall()
        return [dict(row) for row in rows]
