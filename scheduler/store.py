"""
Taurus Scheduler - Task models and storage layer
Reads ScriptTask table directly from MySQL using SQLAlchemy, no dependency on Django ORM
Keeps Scheduler fully decoupled from the main Django application
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from sqlalchemy import Column, DateTime, Integer, String, JSON, Boolean, ForeignKey, MetaData, Table, create_engine, text
from sqlalchemy.orm import sessionmaker

from .config import get_settings

import structlog

logger = structlog.get_logger()


class ScheduleType(str, Enum):
    CRON = "cron"
    INTERVAL = "interval"
    ONCE = "once"


class TaskStatus(str, Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAIL = "fail"


@dataclass
class ScriptTaskRow:
    """Task configuration read from taurus_script_task table (no Django ORM dependency)"""
    id: int
    name: str
    description: Optional[str]
    script_id: int
    schedule_type: ScheduleType
    cron_expression: Optional[str]
    interval_seconds: Optional[int]
    run_once_at: Optional[datetime]
    hosts: list[str] = field(default_factory=list)
    timeout: int = 300
    fail_notify: bool = True
    enabled: bool = True
    envs: dict[str, Any] = field(default_factory=dict)
    args: list[Any] = field(default_factory=list)
    exec_count: int = 0
    last_exec_time: Optional[datetime] = None
    last_exec_result: Optional[str] = None
    next_exec_time: Optional[datetime] = None
    creator_id: Optional[int] = None
    create_datetime: Optional[datetime] = None
    update_datetime: Optional[datetime] = None

    @property
    def job_id(self) -> str:
        return f"script_task:{self.id}"


@dataclass
class WorkflowRow:
    id: int
    name: str
    description: Optional[str]
    schedule_type: ScheduleType
    cron_expression: Optional[str]
    interval_seconds: Optional[int]
    run_once_at: Optional[datetime]
    schedule_enabled: bool = True
    has_schedule: bool = True
    exec_count: int = 0
    last_exec_time: Optional[datetime] = None
    last_exec_result: Optional[str] = None
    next_exec_time: Optional[datetime] = None
    workflow_mode: str = "dag"
    dag_published_version_id: Optional[int] = None
    global_timeout_sec: int = 0
    creator_id: Optional[int] = None
    create_datetime: Optional[datetime] = None
    update_datetime: Optional[datetime] = None

    @property
    def job_id(self) -> str:
        return f"workflow:{self.id}"


@dataclass
class ScheduleRow:
    """Legacy Schedule model (single script / workflow scheduling via Celery Beat).
    Now picked up by taurus-scheduler directly."""
    id: int
    name: str
    description: Optional[str]
    schedule_type: ScheduleType
    cron_expression: Optional[str]
    interval_seconds: Optional[int]
    run_once_at: Optional[datetime]
    target_type: str = "script"
    template_id: Optional[int] = None
    workflow_id: Optional[int] = None
    dag_version_id: Optional[int] = None
    status: int = 1
    envs: dict[str, Any] = field(default_factory=dict)
    args: list[Any] = field(default_factory=list)
    last_run_time: Optional[datetime] = None
    next_run_time: Optional[datetime] = None
    creator_id: Optional[int] = None
    create_datetime: Optional[datetime] = None
    update_datetime: Optional[datetime] = None

    @property
    def job_id(self) -> str:
        return f"schedule:{self.id}"

    @property
    def enabled(self) -> bool:
        return self.status == 1


class TaskStore:
    """Task data access layer: direct access to shared database"""

    def __init__(self):
        self.settings = get_settings()
        self.engine = create_engine(
            self.settings.db_dsn,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False,
        )
        self.Session = sessionmaker(bind=self.engine)
        # Execution record table column cache (populated on first create_execution_record, compatible with un-migrated DB)
        self._execution_columns: Optional[set[str]] = None
        # ScriptTask table column cache (populated on first read/write as needed)
        self._script_task_columns: Optional[set[str]] = None

    # ---------------- ScriptTask Read ----------------
    def list_enabled_script_tasks(self) -> list[ScriptTaskRow]:
        """Get all enabled script scheduled tasks"""
        tbl = self.settings.script_task_table
        sql = text(f"""
            SELECT id, name, description, script_id, schedule_type,
                   cron_expression, interval_seconds, run_once_at,
                   hosts, timeout, fail_notify, enabled, envs, args,
                   exec_count, last_exec_time, last_exec_result, next_exec_time,
                   creator_id, create_datetime, update_datetime
            FROM `{tbl}`
            WHERE enabled = 1
        """)
        rows = []
        with self.Session() as session:
            for r in session.execute(sql).mappings().all():
                rows.append(self._row_to_task(dict(r)))
        return rows

    # ---------------- Status validation (lightweight query for engine pre-dispatch short-circuit) ----------------
    def get_script_status(self, script_id: int) -> Optional[int]:
        """Get script status, returns None if script does not exist"""
        tbl = self.settings.script_table
        sql = text(f"SELECT status FROM `{tbl}` WHERE id = :id")
        with self.Session() as session:
            r = session.execute(sql, {"id": script_id}).first()
            return r[0] if r else None

    def get_workflow_status(self, workflow_id: int) -> Optional[int]:
        """Get workflow status, returns None if workflow does not exist"""
        tbl = self.settings.workflow_table
        sql = text(f"SELECT status FROM `{tbl}` WHERE id = :id")
        with self.Session() as session:
            r = session.execute(sql, {"id": workflow_id}).first()
            return r[0] if r else None

    def get_script_task(self, task_id: int) -> Optional[ScriptTaskRow]:
        tbl = self.settings.script_task_table
        sql = text(f"""
            SELECT id, name, description, script_id, schedule_type,
                   cron_expression, interval_seconds, run_once_at,
                   hosts, timeout, fail_notify, enabled, envs, args,
                   exec_count, last_exec_time, last_exec_result, next_exec_time,
                   creator_id, create_datetime, update_datetime
            FROM `{tbl}`
            WHERE id = :id
        """)
        with self.Session() as session:
            r = session.execute(sql, {"id": task_id}).mappings().first()
            return self._row_to_task(dict(r)) if r else None

    def list_missed_script_tasks(self, grace_seconds: int) -> list[ScriptTaskRow]:
        """Find 'due but not executed' tasks (for compensation on startup)"""
        from datetime import timedelta
        now = datetime.now()
        cutoff = now - timedelta(seconds=grace_seconds)
        tbl = self.settings.script_task_table
        sql = text(f"""
            SELECT id, name, description, script_id, schedule_type,
                   cron_expression, interval_seconds, run_once_at,
                   hosts, timeout, fail_notify, enabled, envs, args,
                   exec_count, last_exec_time, last_exec_result, next_exec_time,
                   creator_id, create_datetime, update_datetime
            FROM `{tbl}`
            WHERE enabled = 1
              AND next_exec_time IS NOT NULL
              AND next_exec_time >= :cutoff
              AND next_exec_time <= :now
              AND (last_exec_time IS NULL OR last_exec_time < next_exec_time)
        """)
        rows = []
        with self.Session() as session:
            for r in session.execute(sql, {"cutoff": cutoff, "now": now}).mappings().all():
                rows.append(self._row_to_task(dict(r)))
        return rows

    # ---------------- ScriptTask Status Write-back ----------------
    def update_task_after_execution(
        self,
        task_id: int,
        status: TaskStatus,
        next_exec_time: Optional[datetime] = None,
    ) -> None:
        """Write back execution info after task trigger (only writes execution count, last time, result, next time)"""
        tbl = self.settings.script_task_table
        now = datetime.now()
        fields = [
            "last_exec_time = :now",
            "last_exec_result = :result",
            "exec_count = exec_count + 1",
        ]
        params = {
            "id": task_id,
            "now": now,
            "result": status.value,
        }
        if next_exec_time is not None:
            fields.append("next_exec_time = :next")
            params["next"] = next_exec_time
        sql = text(f"UPDATE `{tbl}` SET {', '.join(fields)} WHERE id = :id")
        with self.Session() as session:
            session.execute(sql, params)
            session.commit()

    def update_task_next_exec_time(self, task_id: int, next_exec_time: Optional[datetime]) -> None:
        tbl = self.settings.script_task_table
        sql = text(f"UPDATE `{tbl}` SET next_exec_time = :next WHERE id = :id")
        with self.Session() as session:
            session.execute(sql, {"id": task_id, "next": next_exec_time})
            session.commit()

    def disable_task(self, task_id: int) -> None:
        """Disable a task (e.g. expired one-time tasks)"""
        tbl = self.settings.script_task_table
        sql = text(f"UPDATE `{tbl}` SET enabled = 0 WHERE id = :id")
        with self.Session() as session:
            session.execute(sql, {"id": task_id})
            session.commit()

    # ---------------- ScriptTaskExecution Write ----------------
    def create_execution_record(
        self,
        task_id: int,
        trigger_type: str = "schedule",
        scheduled_fire_time: Optional[datetime] = None,
        creator_id: Optional[int] = None,
    ) -> int:
        """Create a script scheduled task execution record, returns the record ID.
        Compatible with old un-migrated DB: skips scheduled_fire_time column if it does not exist.
        creator_id: pass ScriptTask.creator_id for scheduled triggers (the task creator owns the scheduled execution);
                    if None, sets NULL when column allows it or falls back to 1 (avoids NOT NULL errors)."""
        tbl = self.settings.script_task_execution_table
        now = datetime.now()
        if self._execution_columns is None:
            try:
                with self.engine.connect() as conn:
                    rs = conn.execute(text(f"SHOW COLUMNS FROM `{tbl}`"))
                    self._execution_columns = {row[0] for row in rs}
            except Exception:
                self._execution_columns = set()

        cols = ["task_id", "status", "start_time", "trigger_type",
                "executed_hosts", "create_datetime", "update_datetime"]
        vals = {
            "task_id": task_id,
            "status": 1,
            "start_time": now,
            "trigger_type": trigger_type,
            "executed_hosts": "[]",
            "create_time": now,
            "update_time": now,
        }
        if "scheduled_fire_time" in self._execution_columns:
            cols.append("scheduled_fire_time")
            vals["scheduled_fire_time"] = scheduled_fire_time or now
        # creator/modifier/dept_belong (actual DB column names are creator_id / modifier_id / dept_belong_id,
        #   Django ForeignKey auto-appends _id suffix; original extra used 'creator' string so never matched → previously scheduled triggers had creator_id all NULL)
        #   Scheduled trigger: assign "task creator's own creator_id", same semantics as manual execution; if empty fall back to existing admin id
        #     (Users table always has admin/superadmin, use 2 or 1 if not found)
        creator_val = creator_id if creator_id else None
        if creator_val is None:
            # Don't rush to write None: if DB column is NOT NULL, None will fail insertion. Fall back to an id that must exist in Django users
            try:
                with self.Session() as s:
                    tbl_user = getattr(self.settings, 'AUTH_USER_MODEL_TABLE', None)
                    if tbl_user is None:
                        # Guess dvadmin default users table is dvadmin_users or directly users
                        tbl_user = 'dvadmin_users'
                    # Get the smallest id (usually superadmin=1, admin=2), ensure it exists
                    r = s.execute(text(f"SELECT id FROM `{tbl_user}` ORDER BY id LIMIT 1")).fetchone()
                    if r is None:
                        # Try auth_user (Django default)
                        try:
                            r = s.execute(text("SELECT id FROM auth_user ORDER BY id LIMIT 1")).fetchone()
                        except Exception:
                            r = None
                    if r is not None:
                        creator_val = int(r[0])
            except Exception:
                creator_val = None
            if creator_val is None:
                creator_val = 1

        # (semantic name → actual DB column name) mapping, try both sides (different migration histories may have slightly different column names)
        extra_map = [
            # (db_col_candidates,            val_when_match)
            (("creator_id", "creator"),       creator_val),
            (("modifier_id", "modifier"),     creator_val),
            (("dept_belong_id", "dept_belong", "belong_dept"), ""),
            (("belong_unit",),                ""),
        ]
        for db_candidates, val in extra_map:
            real_col = None
            for cand in db_candidates:
                if cand in self._execution_columns:
                    real_col = cand
                    break
            if real_col is None:
                continue
            cols.append(real_col)
            vals[real_col] = val

        placeholders = ", ".join([f":{c if c not in ('create_datetime','update_datetime') else c.replace('_datetime','_time')}" for c in cols])
        col_list = ", ".join([f"`{c}`" for c in cols])
        sql = text(f"INSERT INTO `{tbl}` ({col_list}) VALUES ({placeholders})")
        params = {
            (c if c not in ("create_datetime", "update_datetime") else c.replace("_datetime", "_time")): vals.get(
                c if c in vals else (c.replace("_datetime", "_time")))
            for c in cols
        }
        with self.Session() as session:
            result = session.execute(sql, params)
            session.commit()
            return result.lastrowid

    def mark_execution_done(
        self,
        execution_id: int,
        status: TaskStatus,
        result: Optional[dict] = None,
        error_message: Optional[str] = None,
        executed_hosts: Optional[list[str]] = None,
    ) -> None:
        tbl = self.settings.script_task_execution_table
        now = datetime.now()
        sql = text(f"""
            UPDATE `{tbl}`
            SET status = :status,
                end_time = :end_time,
                duration = TIMESTAMPDIFF(SECOND, start_time, :end_time),
                result = :result,
                error_message = :error_message,
                executed_hosts = :hosts,
                update_datetime = :update_time
            WHERE id = :id
        """)
        with self.Session() as session:
            session.execute(sql, {
                "id": execution_id,
                "status": 2 if status == TaskStatus.SUCCESS else 3,
                "end_time": now,
                "result": json.dumps(result or {}, ensure_ascii=False),
                "error_message": error_message,
                "hosts": json.dumps(executed_hosts or [], ensure_ascii=False),
                "update_time": now,
            })
            session.commit()

    # ---------------- Schedule table (legacy compatibility) ----------------
    def list_enabled_schedules(self) -> list[dict]:
        tbl = self.settings.schedule_table
        sql = text(f"""
            SELECT id, name, schedule_type, cron_expression, interval_seconds,
                   run_once_at, target_type, template_id, workflow_id,
                   status, envs, args, last_run_time, next_run_time
            FROM `{tbl}`
            WHERE status = 1
        """)
        try:
            with self.Session() as session:
                return [dict(r) for r in session.execute(sql).mappings().all()]
        except Exception as e:
            logger.warning("schedule.table.not_found_or_error", error=str(e))
            return []

    # ---------------- Workflow Read ----------------
    def list_enabled_workflows(self) -> list[WorkflowRow]:
        tbl = self.settings.workflow_table
        sql = text(f"""
            SELECT id, name, description, schedule_type,
                   cron_expression, interval_seconds, run_once_at,
                   schedule_enabled, has_schedule,
                   exec_count, last_exec_time, last_exec_result, next_exec_time,
                   workflow_mode, dag_published_version_id, global_timeout_sec,
                   creator_id, create_datetime, update_datetime
            FROM `{tbl}`
            WHERE has_schedule = 1 AND schedule_enabled = 1
        """)
        rows = []
        with self.Session() as session:
            for r in session.execute(sql).mappings().all():
                rows.append(self._row_to_workflow(dict(r)))
        return rows

    def get_workflow(self, workflow_id: int) -> Optional[WorkflowRow]:
        tbl = self.settings.workflow_table
        sql = text(f"""
            SELECT id, name, description, schedule_type,
                   cron_expression, interval_seconds, run_once_at,
                   schedule_enabled, has_schedule,
                   exec_count, last_exec_time, last_exec_result, next_exec_time,
                   workflow_mode, dag_published_version_id, global_timeout_sec,
                   creator_id, create_datetime, update_datetime
            FROM `{tbl}`
            WHERE id = :id AND has_schedule = 1
        """)
        with self.Session() as session:
            r = session.execute(sql, {"id": workflow_id}).mappings().first()
            return self._row_to_workflow(dict(r)) if r else None

    def list_missed_workflows(self, grace_seconds: int) -> list[WorkflowRow]:
        from datetime import timedelta
        now = datetime.now()
        cutoff = now - timedelta(seconds=grace_seconds)
        tbl = self.settings.workflow_table
        sql = text(f"""
            SELECT id, name, description, schedule_type,
                   cron_expression, interval_seconds, run_once_at,
                   schedule_enabled, has_schedule,
                   exec_count, last_exec_time, last_exec_result, next_exec_time,
                   workflow_mode, dag_published_version_id, global_timeout_sec,
                   creator_id, create_datetime, update_datetime
            FROM `{tbl}`
            WHERE has_schedule = 1 AND schedule_enabled = 1
              AND next_exec_time IS NOT NULL
              AND next_exec_time >= :cutoff
              AND next_exec_time <= :now
              AND (last_exec_time IS NULL OR last_exec_time < next_exec_time)
        """)
        rows = []
        with self.Session() as session:
            for r in session.execute(sql, {"cutoff": cutoff, "now": now}).mappings().all():
                rows.append(self._row_to_workflow(dict(r)))
        return rows

    # ---------------- Workflow Status Write-back ----------------
    def update_workflow_after_execution(
        self,
        workflow_id: int,
        status: TaskStatus,
        next_exec_time: Optional[datetime] = None,
    ) -> None:
        tbl = self.settings.workflow_table
        now = datetime.now()
        fields = [
            "last_exec_time = :now",
            "last_exec_result = :result",
            "exec_count = exec_count + 1",
        ]
        params = {
            "id": workflow_id,
            "now": now,
            "result": status.value,
        }
        if next_exec_time is not None:
            fields.append("next_exec_time = :next")
            params["next"] = next_exec_time
        sql = text(f"UPDATE `{tbl}` SET {', '.join(fields)} WHERE id = :id")
        with self.Session() as session:
            session.execute(sql, params)
            session.commit()

    def update_workflow_next_exec_time(self, workflow_id: int, next_exec_time: Optional[datetime]) -> None:
        tbl = self.settings.workflow_table
        sql = text(f"UPDATE `{tbl}` SET next_exec_time = :next WHERE id = :id")
        with self.Session() as session:
            session.execute(sql, {"id": workflow_id, "next": next_exec_time})
            session.commit()

    def disable_workflow_schedule(self, workflow_id: int) -> None:
        tbl = self.settings.workflow_table
        sql = text(f"UPDATE `{tbl}` SET schedule_enabled = 0 WHERE id = :id")
        with self.Session() as session:
            session.execute(sql, {"id": workflow_id})
            session.commit()

    # ---------------- ScheduleExecution Write (Workflow scheduling) ----------------
    def create_schedule_execution_record(
        self,
        schedule_id: int,
        workflow_id: int,
        trigger_type: str = "schedule",
        scheduled_fire_time: Optional[datetime] = None,
        creator_id: Optional[int] = None,
    ) -> int:
        tbl = self.settings.schedule_execution_table
        now = datetime.now()
        cols = ["schedule_id", "status", "start_time",
                "create_datetime", "update_datetime", "creator_id", "modifier",
                "result", "error_message"]
        vals = {
            "schedule_id": schedule_id,
            "status": 1,
            "start_time": now,
            "create_datetime": now,
            "update_datetime": now,
            "creator_id": creator_id or 1,
            "modifier": str(creator_id or 1),
            "result": "{}",
            "error_message": "",
        }

        placeholders = ", ".join([f":{c}" for c in cols])
        col_list = ", ".join([f"`{c}`" for c in cols])
        sql = text(f"INSERT INTO `{tbl}` ({col_list}) VALUES ({placeholders})")
        with self.Session() as session:
            result = session.execute(sql, vals)
            session.commit()
            return result.lastrowid

    def mark_schedule_execution_done(
        self,
        execution_id: int,
        status: TaskStatus,
        result: Optional[dict] = None,
        error_message: Optional[str] = None,
    ) -> None:
        tbl = self.settings.schedule_execution_table
        now = datetime.now()
        sql = text(f"""
            UPDATE `{tbl}`
            SET status = :status,
                end_time = :end_time,
                result = :result,
                error_message = :error_message,
                update_datetime = :update_time
            WHERE id = :id
        """)
        with self.Session() as session:
            session.execute(sql, {
                "id": execution_id,
                "status": 2 if status == TaskStatus.SUCCESS else 3,
                "end_time": now,
                "result": json.dumps(result or {}, ensure_ascii=False),
                "error_message": error_message,
                "update_time": now,
            })
            session.commit()

    # ---------------- Helpers ----------------
    @staticmethod
    def _row_to_task(r: dict) -> ScriptTaskRow:
        def parse_json(val):
            if val is None:
                return None
            if isinstance(val, (dict, list)):
                return val
            try:
                return json.loads(val)
            except Exception:
                return val

        return ScriptTaskRow(
            id=int(r["id"]),
            name=str(r["name"]),
            description=r.get("description"),
            script_id=int(r["script_id"]),
            schedule_type=ScheduleType(r.get("schedule_type") or "cron"),
            cron_expression=r.get("cron_expression"),
            interval_seconds=int(r["interval_seconds"]) if r.get("interval_seconds") is not None else None,
            run_once_at=r.get("run_once_at"),
            hosts=parse_json(r.get("hosts")) or [],
            timeout=int(r.get("timeout") or 300),
            fail_notify=bool(r.get("fail_notify", True)),
            enabled=bool(r.get("enabled", True)),
            envs=parse_json(r.get("envs")) or {},
            args=parse_json(r.get("args")) or [],
            exec_count=int(r.get("exec_count") or 0),
            last_exec_time=r.get("last_exec_time"),
            last_exec_result=r.get("last_exec_result"),
            next_exec_time=r.get("next_exec_time"),
            creator_id=int(r["creator_id"]) if r.get("creator_id") else None,
            create_datetime=r.get("create_datetime"),
            update_datetime=r.get("update_datetime"),
        )

    @staticmethod
    def _row_to_workflow(r: dict) -> WorkflowRow:
        return WorkflowRow(
            id=int(r["id"]),
            name=str(r["name"]),
            description=r.get("description"),
            schedule_type=ScheduleType(r.get("schedule_type") or "once"),
            cron_expression=r.get("cron_expression"),
            interval_seconds=int(r["interval_seconds"]) if r.get("interval_seconds") is not None else None,
            run_once_at=r.get("run_once_at"),
            schedule_enabled=bool(r.get("schedule_enabled", True)),
            has_schedule=bool(r.get("has_schedule", True)),
            exec_count=int(r.get("exec_count") or 0),
            last_exec_time=r.get("last_exec_time"),
            last_exec_result=r.get("last_exec_result"),
            next_exec_time=r.get("next_exec_time"),
            workflow_mode=str(r.get("workflow_mode") or "dag"),
            dag_published_version_id=int(r["dag_published_version_id"]) if r.get("dag_published_version_id") is not None else None,
            global_timeout_sec=int(r.get("global_timeout_sec") or 0),
            creator_id=int(r["creator_id"]) if r.get("creator_id") else None,
            create_datetime=r.get("create_datetime"),
            update_datetime=r.get("update_datetime"),
        )

    # ---------------- Legacy Schedule table ----------------
    def list_enabled_schedules(self) -> list[ScheduleRow]:
        tbl = self.settings.schedule_table
        sql = text(f"""
            SELECT id, name, description, schedule_type,
                   cron_expression, interval_seconds, run_once_at,
                   target_type, template_id, workflow_id, dag_version_id,
                   status, envs, args,
                   last_run_time, next_run_time,
                   creator_id, create_datetime, update_datetime
            FROM `{tbl}`
            WHERE status = 1
        """)
        rows = []
        with self.Session() as session:
            for r in session.execute(sql).mappings().all():
                rows.append(self._row_to_schedule(dict(r)))
        return rows

    def get_schedule(self, schedule_id: int) -> Optional[ScheduleRow]:
        tbl = self.settings.schedule_table
        sql = text(f"""
            SELECT id, name, description, schedule_type,
                   cron_expression, interval_seconds, run_once_at,
                   target_type, template_id, workflow_id, dag_version_id,
                   status, envs, args,
                   last_run_time, next_run_time,
                   creator_id, create_datetime, update_datetime
            FROM `{tbl}`
            WHERE id = :id
        """)
        with self.Session() as session:
            r = session.execute(sql, {"id": schedule_id}).mappings().first()
            return self._row_to_schedule(dict(r)) if r else None

    def list_missed_schedules(self, grace_seconds: int) -> list[ScheduleRow]:
        from datetime import timedelta
        now = datetime.now()
        cutoff = now - timedelta(seconds=grace_seconds)
        tbl = self.settings.schedule_table
        sql = text(f"""
            SELECT id, name, description, schedule_type,
                   cron_expression, interval_seconds, run_once_at,
                   target_type, template_id, workflow_id, dag_version_id,
                   status, envs, args,
                   last_run_time, next_run_time,
                   creator_id, create_datetime, update_datetime
            FROM `{tbl}`
            WHERE status = 1
              AND next_run_time IS NOT NULL
              AND next_run_time >= :cutoff
              AND next_run_time <= :now
              AND (last_run_time IS NULL OR last_run_time < next_run_time)
        """)
        rows = []
        with self.Session() as session:
            for r in session.execute(sql, {"cutoff": cutoff, "now": now}).mappings().all():
                rows.append(self._row_to_schedule(dict(r)))
        return rows

    def update_schedule_after_execution(
        self,
        schedule_id: int,
        next_run_time: Optional[datetime] = None,
    ) -> None:
        tbl = self.settings.schedule_table
        now = datetime.now()
        fields = ["last_run_time = :now"]
        params: dict[str, Any] = {"id": schedule_id, "now": now}
        if next_run_time is not None:
            fields.append("next_run_time = :next")
            params["next"] = next_run_time
        sql = text(f"UPDATE `{tbl}` SET {', '.join(fields)} WHERE id = :id")
        with self.Session() as session:
            session.execute(sql, params)
            session.commit()

    def disable_schedule_once(self, schedule_id: int) -> None:
        tbl = self.settings.schedule_table
        sql = text(f"UPDATE `{tbl}` SET status = 0 WHERE id = :id AND schedule_type = 'once'")
        with self.Session() as session:
            session.execute(sql, {"id": schedule_id})
            session.commit()

    @staticmethod
    def _row_to_schedule(r: dict) -> ScheduleRow:
        return ScheduleRow(
            id=int(r["id"]),
            name=str(r["name"]),
            description=r.get("description"),
            schedule_type=ScheduleType(r.get("schedule_type") or "once"),
            cron_expression=r.get("cron_expression"),
            interval_seconds=int(r["interval_seconds"]) if r.get("interval_seconds") is not None else None,
            run_once_at=r.get("run_once_at"),
            target_type=str(r.get("target_type") or "script"),
            template_id=int(r["template_id"]) if r.get("template_id") else None,
            workflow_id=int(r["workflow_id"]) if r.get("workflow_id") else None,
            dag_version_id=int(r["dag_version_id"]) if r.get("dag_version_id") else None,
            status=int(r.get("status") or 0),
            envs=r.get("envs") or {},
            args=r.get("args") or [],
            last_run_time=r.get("last_run_time"),
            next_run_time=r.get("next_run_time"),
            creator_id=int(r["creator_id"]) if r.get("creator_id") else None,
            create_datetime=r.get("create_datetime"),
            update_datetime=r.get("update_datetime"),
        )

    def close(self) -> None:
        self.engine.dispose()