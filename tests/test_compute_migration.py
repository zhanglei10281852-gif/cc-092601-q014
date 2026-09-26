from __future__ import annotations

import sqlite3

from app.compute.service import ComputeOperationsService
from app.core.clock import to_storage, utc_now
from app.database import close_connection, get_connection, migrate_db
from tests.test_compute_operations import TEMPLATE, submit_payload


OLD_COMPUTE_SCHEMA = """
CREATE TABLE compute_templates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    algorithm TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    parameter_schema_json TEXT NOT NULL,
    default_parameters_json TEXT NOT NULL DEFAULT '{}',
    max_runtime_seconds INTEGER NOT NULL CHECK(max_runtime_seconds > 0),
    max_attempts INTEGER NOT NULL CHECK(max_attempts > 0),
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE compute_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    template_id INTEGER NOT NULL REFERENCES compute_templates(id) ON DELETE RESTRICT,
    project_code TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    parameters_json TEXT NOT NULL,
    parameter_digest TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 50 CHECK(priority BETWEEN 0 AND 100),
    idempotency_key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','cancel_requested','cancelled','succeeded','failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL CHECK(max_attempts > 0),
    available_at TEXT NOT NULL,
    lease_owner TEXT NOT NULL DEFAULT '',
    lease_expires_at TEXT NOT NULL DEFAULT '',
    current_result_version INTEGER,
    last_error_code TEXT NOT NULL DEFAULT '',
    last_error_message TEXT NOT NULL DEFAULT '',
    version INTEGER NOT NULL DEFAULT 1,
    started_at TEXT,
    finished_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(requested_by, idempotency_key)
);
CREATE TABLE compute_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL REFERENCES compute_tasks(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    result_json TEXT NOT NULL,
    metrics_json TEXT NOT NULL DEFAULT '{}',
    result_digest TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(task_id, version)
);
CREATE TABLE compute_interventions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL REFERENCES compute_tasks(id) ON DELETE CASCADE,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    reason TEXT NOT NULL,
    before_json TEXT NOT NULL,
    after_json TEXT NOT NULL,
    batch_key TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
"""


def test_legacy_database_migrates_and_supports_result_revoked(tmp_path, monkeypatch):
    db_path = tmp_path / "legacy.db"
    monkeypatch.setenv("TOWNSHIP_DATABASE_PATH", str(db_path))
    close_connection()

    # 用裸连接构造一个“旧版”数据库
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.executescript(OLD_COMPUTE_SCHEMA)
    now = to_storage(utc_now())
    import json
    connection.execute(
        "INSERT INTO compute_templates(code,name,algorithm,version,parameter_schema_json,default_parameters_json,max_runtime_seconds,max_attempts,active,created_by,created_at,updated_at) VALUES(?,?,?,1,?,?,300,2,1,'administrator',?,?)",
        (TEMPLATE["code"], TEMPLATE["name"], TEMPLATE["algorithm"], json.dumps(TEMPLATE["parameter_schema"], sort_keys=True),
         json.dumps(TEMPLATE["default_parameters"], sort_keys=True), now, now),
    )
    connection.execute(
        "INSERT INTO compute_tasks(template_id,project_code,requested_by,parameters_json,parameter_digest,priority,idempotency_key,status,attempt_count,max_attempts,available_at,current_result_version,created_at,updated_at) VALUES(1,'project-a','legacy-user','{}','d',80,'legacy-key-000001','succeeded',1,2,?,1,?,?)",
        (now, now, now),
    )
    connection.execute(
        "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,created_by,created_at) VALUES(1,1,'{}','{}','rd','w1',?)",
        (now,),
    )
    connection.commit()
    connection.close()

    # 旧约束下 result_revoked 应被拒绝
    guard = sqlite3.connect(db_path)
    import pytest
    with pytest.raises(sqlite3.IntegrityError):
        guard.execute("UPDATE compute_tasks SET status='result_revoked' WHERE id=1")
    guard.close()

    # 执行迁移
    migrate_db()
    migrated = get_connection()
    policies = migrated.execute("SELECT COUNT(*) AS amount FROM compute_approval_policies").fetchone()["amount"]
    assert policies == 4
    # 迁移后新状态可用，数据与版本号保留
    service = ComputeOperationsService(migrated)
    result = service.withdraw_result(1, "alice", "旧库迁移后撤回")
    assert result["status"] == "result_revoked" and int(result["version"]) == 2
    detail = service.get_task(1)
    assert detail["requested_by"] == "legacy-user"
    assert detail["results"][0]["withdraw_reason"] == "旧库迁移后撤回"
    intervention = migrated.execute("SELECT request_id FROM compute_interventions WHERE action='result_withdraw'").fetchone()
    assert intervention["request_id"] is None

    # 迁移应幂等：再次执行不报错、不重建
    migrate_db()
    close_connection()
