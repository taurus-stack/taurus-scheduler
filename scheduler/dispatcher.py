"""
Taurus Scheduler - Task dispatcher
When due:
1. Write ScriptTaskExecution record to DB
2. Update ScriptTask execution count and time
3. Push to Redis Queue (Worker consumes or Backend API callback
4. Optional: direct callback to taurus-backend internal execution interface
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import time
from datetime import datetime
from typing import Any, Optional

import redis

import httpx

from .config import get_settings
from .store import ScriptTaskRow, WorkflowRow, TaskStore, TaskStatus

import structlog

logger = structlog.get_logger()


class TaskDispatcher:
    """Task dispatcher: triggers due tasks to the execution plane"""

    def __init__(self, redis_client: redis.Redis, store: TaskStore):
        self.settings = get_settings()
        self.redis = redis_client
        self.store = store

    # ---------- Public Entry ----------
    async def dispatch(
        self,
        task: ScriptTaskRow,
        scheduled_fire_time: Optional[datetime] = None,
        trigger_type: str = "schedule",
        force: bool = False,
    ) -> bool:
        """
        Dispatch a task
        :return: Whether dispatched successfully (after idempotent dedup, duplicate dispatch returns False)
        """
        # 1. Idempotent deduplication: same task ID + same minute granularity only allows one trigger
        dedup_key = self._dedup_key(task, scheduled_fire_time)
        if not force:
            if self.redis.set(dedup_key, "1", nx=True, ex=self.settings.SCHEDULER_DEDUP_TTL):
                logger.debug("dedup.pass", task_id=task.id, key=dedup_key)
            else:
                logger.info("dedup.skipped", task_id=task.id, key=dedup_key)
                return False

        try:
            # 2. Write DB: create execution record (scheduled trigger → creator assigned to "task creator", same semantics as manual execution)
            execution_id = self.store.create_execution_record(
                task_id=task.id,
                trigger_type=trigger_type,
                scheduled_fire_time=scheduled_fire_time,
                creator_id=getattr(task, 'creator_id', None),
            )
            logger.info("dispatcher.record_created",
                        task_id=task.id,
                        execution_id=execution_id,
                        trigger_type=trigger_type)

            # 3. Build task Payload
            payload = self._build_payload(task, execution_id, scheduled_fire_time)

            # 4. Push for execution (Redis Queue first, optional HTTP callback fallback)
            dispatched_ok = False
            try:
                dispatched_ok = await self._push_to_queue(payload)
            except Exception as e:
                logger.error("dispatcher.queue_push_failed", task_id=task.id, error=str(e))

            if not dispatched_ok and self.settings.BACKEND_API_BASE_URL:
                # Queue failed, fall back to direct backend API call
                try:
                    dispatched_ok = await self._callback_backend_api(payload)
                except Exception as e:
                    logger.error("dispatcher.callback_failed", task_id=task.id, error=str(e))

            # 5. Regardless of dispatch success, update task's last execution time
            #    Actual execution status (success/failure written back by Worker after execution completes)
            self.store.update_task_after_execution(
                task_id=task.id,
                status=TaskStatus.PENDING,
                next_exec_time=None,  # Engine will recalculate
            )

            if not dispatched_ok:
                # Both methods failed, mark execution record as failed
                self.store.mark_execution_done(
                    execution_id=execution_id,
                    status=TaskStatus.FAIL,
                    error_message="Task queue unreachable and backend callback failed, scheduler cannot trigger execution",
                )
                logger.error("dispatcher.dispatch_failed", task_id=task.id, execution_id=execution_id)
                return False

            logger.info("dispatcher.dispatched",
                        task_id=task.id,
                        execution_id=execution_id,
                        task_name=task.name)
            return True

        except Exception as e:
            logger.exception("dispatcher.unexpected_error", task_id=task.id, error=str(e))
            return False

    # ---------- Internal ----------
    def _dedup_key(self, task: ScriptTaskRow, scheduled_fire_time: Optional[datetime]) -> str:
        """Deduplication key: task_id + fire_time minute-level granularity"""
        ft = scheduled_fire_time or datetime.now()
        minute_str = ft.strftime("%Y%m%d%H%M")
        raw = f"{task.id}:{minute_str}"
        return f"{self.settings.REDIS_DEDUP_PREFIX}{hashlib.md5(raw.encode()).hexdigest()}"

    @staticmethod
    def _build_payload(
        task: ScriptTaskRow,
        execution_id: int,
        scheduled_fire_time: Optional[datetime],
    ) -> dict[str, Any]:
        def _dt(val):
            return val.isoformat() if isinstance(val, datetime) else None

        return {
            "source": "taurus-scheduler",
            "v": 1,
            "dispatched_at": datetime.now().isoformat(),
            "scheduled_fire_time": _dt(scheduled_fire_time),
            "execution": {
                "id": execution_id,
                "trigger_type": "schedule",
            },
            "task": {
                "id": task.id,
                "name": task.name,
                "schedule_type": task.schedule_type.value,
                "script_id": task.script_id,
                "hosts": task.hosts,
                "timeout": task.timeout,
                "fail_notify": task.fail_notify,
                "envs": task.envs,
                "args": task.args,
                "creator_id": task.creator_id,
            },
        }

    async def _push_to_queue(self, payload: dict[str, Any]) -> bool:
        """Push into Redis Stream / List, decouple from execution plane"""
        key = self.settings.REDIS_QUEUE_KEY
        msg = json.dumps(payload, ensure_ascii=False)

        # List mode: LPUSH + BRPOP consumption (simple and reliable)
        # Trim first then push, to avoid newly pushed messages being dropped by LTRIM
        self.redis.ltrim(key, 0, 9999)  # Limit queue length to prevent explosion
        pushed = self.redis.lpush(key, msg)
        if pushed <= 0:
            return False
        return True

    async def _callback_backend_api(self, payload: dict[str, Any]) -> bool:
        """Fallback to direct taurus-backend API call when Redis Queue consumer is not ready"""
        if not self.settings.BACKEND_API_BASE_URL:
            return False
        task_id = payload["task"]["id"]
        execution_id = payload["execution"]["id"]
        # Interface corresponding to backend ScriptTaskViewSet.execute_now (internal HTTP trigger)
        url = f"/api/taurus/script_task/{task_id}/execute/"
        logger.info("dispatcher.callback_backend", url=url, task_id=task_id)
        headers = {}
        if self.settings.BACKEND_API_TOKEN:
            headers["Authorization"] = f"Bearer {self.settings.BACKEND_API_TOKEN}"
        timeout = httpx.Timeout(10.0, connect=5.0)
        try:
            async with httpx.AsyncClient(
                base_url=self.settings.BACKEND_API_BASE_URL or "",
                headers=headers,
                timeout=timeout,
            ) as client:
                resp = await client.post(url, json={"execution_id": execution_id})
                if resp.status_code in (200, 201):
                    data = resp.json()
                    return data.get("code") == 2000 or data.get("success", False) or resp.status_code == 200
                logger.warning("dispatcher.callback_bad_status",
                               task_id=task_id, status=resp.status_code, body=resp.text[:500])
                return False
        except Exception as e:
            logger.error("dispatcher.callback_exception", task_id=task_id, error=str(e))
            return False

    async def close(self) -> None:
        pass

    # ---------- Workflow Dispatch ----------
    async def dispatch_workflow(
        self,
        wf: WorkflowRow,
        scheduled_fire_time: Optional[datetime] = None,
        trigger_type: str = "schedule",
        force: bool = False,
    ) -> bool:
        """
        Dispatch Workflow scheduled task: push to workflow Redis queue
        """
        dedup_key = self._workflow_dedup_key(wf, scheduled_fire_time)
        if not force:
            if self.redis.set(dedup_key, "1", nx=True, ex=self.settings.SCHEDULER_DEDUP_TTL):
                logger.debug("wf_dedup.pass", workflow_id=wf.id, key=dedup_key)
            else:
                logger.info("wf_dedup.skipped", workflow_id=wf.id, key=dedup_key)
                return False

        try:
            schedule_id = self._ensure_schedule_for_workflow(wf)

            execution_id = self.store.create_schedule_execution_record(
                schedule_id=schedule_id,
                workflow_id=wf.id,
                trigger_type=trigger_type,
                scheduled_fire_time=scheduled_fire_time,
                creator_id=wf.creator_id,
            )
            logger.info("dispatcher.workflow_record_created",
                        workflow_id=wf.id,
                        schedule_id=schedule_id,
                        execution_id=execution_id,
                        trigger_type=trigger_type)

            payload = self._build_workflow_payload(wf, schedule_id, execution_id, scheduled_fire_time)

            dispatched_ok = False
            try:
                dispatched_ok = await self._push_workflow_to_queue(payload)
            except Exception as e:
                logger.error("dispatcher.wf_queue_push_failed", workflow_id=wf.id, error=str(e))

            if not dispatched_ok and self.settings.BACKEND_API_BASE_URL:
                try:
                    dispatched_ok = await self._callback_backend_api(payload, is_workflow=True)
                except Exception as e:
                    logger.error("dispatcher.wf_callback_failed", workflow_id=wf.id, error=str(e))

            self.store.update_workflow_after_execution(
                workflow_id=wf.id,
                status=TaskStatus.PENDING,
                next_exec_time=None,
            )

            if not dispatched_ok:
                self.store.mark_schedule_execution_done(
                    execution_id=execution_id,
                    status=TaskStatus.FAIL,
                    error_message="Workflow queue unreachable and backend callback failed",
                )
                logger.error("dispatcher.workflow_dispatch_failed", workflow_id=wf.id, execution_id=execution_id)
                return False

            logger.info("dispatcher.workflow_dispatched",
                        workflow_id=wf.id,
                        schedule_id=schedule_id,
                        execution_id=execution_id)
            return True

        except Exception as e:
            logger.exception("dispatcher.workflow_unexpected_error", workflow_id=wf.id, error=str(e))
            return False

    def _workflow_dedup_key(self, wf: WorkflowRow, scheduled_fire_time: Optional[datetime]) -> str:
        ft = scheduled_fire_time or datetime.now()
        minute_str = ft.strftime("%Y%m%d%H%M")
        raw = f"wf:{wf.id}:{minute_str}"
        return f"{self.settings.REDIS_DEDUP_PREFIX}{hashlib.md5(raw.encode()).hexdigest()}"

    def _ensure_schedule_for_workflow(self, wf: WorkflowRow) -> int:
        """Ensure corresponding record exists in Schedule table, returns schedule_id"""
        from sqlalchemy import text
        tbl = self.settings.schedule_table
        with self.store.Session() as session:
            existing = session.execute(
                text(f"SELECT id FROM `{tbl}` WHERE target_type = 'workflow' AND workflow_id = :wid AND status = 1 LIMIT 1"),
                {"wid": wf.id}
            ).first()
            if existing:
                return int(existing[0])

            now = datetime.now()
            sql = text(f"INSERT INTO `{tbl}` "
                       f"(name, description, schedule_type, cron_expression, "
                       f"interval_seconds, run_once_at, target_type, workflow_id, status, "
                       f"envs, args, create_datetime, update_datetime, creator_id, modifier) "
                       f"VALUES (:name, :desc, :st, :ce, :is, :roa, 'workflow', :wid, 1, "
                       f"'{{}}', '[]', :now, :now, :creator, :modifier)")
            result = session.execute(sql, {
                "name": f"{wf.name} scheduled task",
                "desc": wf.description or "",
                "st": wf.schedule_type.value,
                "ce": wf.cron_expression or "",
                "is": wf.interval_seconds,
                "roa": wf.run_once_at,
                "wid": wf.id,
                "now": now,
                "creator": wf.creator_id or 1,
                "modifier": str(wf.creator_id or 1),
            })
            session.commit()
            return int(result.lastrowid)

    @staticmethod
    def _build_workflow_payload(
        wf: WorkflowRow,
        schedule_id: int,
        execution_id: int,
        scheduled_fire_time: Optional[datetime],
    ) -> dict[str, Any]:
        def _dt(val):
            return val.isoformat() if isinstance(val, datetime) else None

        return {
            "source": "taurus-scheduler",
            "v": 1,
            "dispatched_at": datetime.now().isoformat(),
            "scheduled_fire_time": _dt(scheduled_fire_time),
            "target_type": "workflow",
            "schedule": {
                "id": schedule_id,
                "target_type": "workflow",
                "trigger_type": "schedule",
            },
            "workflow": {
                "id": wf.id,
                "name": wf.name,
                "workflow_mode": wf.workflow_mode,
                "dag_published_version_id": wf.dag_published_version_id,
                "global_timeout_sec": wf.global_timeout_sec,
                "creator_id": wf.creator_id,
            },
        }

    async def _push_workflow_to_queue(self, payload: dict[str, Any]) -> bool:
        key = self.settings.REDIS_WORKFLOW_QUEUE_KEY
        msg = json.dumps(payload, ensure_ascii=False)
        self.redis.ltrim(key, 0, 9999)
        pushed = self.redis.lpush(key, msg)
        return pushed > 0

    async def _callback_backend_api(
        self, payload: dict[str, Any], is_workflow: bool = False
    ) -> bool:
        if not self.settings.BACKEND_API_BASE_URL:
            return False
        if is_workflow:
            wf_id = payload["workflow"]["id"]
            url = f"/api/taurus/workflow/{wf_id}/execute/"
        else:
            task_id = payload["task"]["id"]
            url = f"/api/taurus/script_task/{task_id}/execute/"
        logger.info("dispatcher.callback_backend", url=url, is_workflow=is_workflow)
        headers = {}
        if self.settings.BACKEND_API_TOKEN:
            headers["Authorization"] = f"Bearer {self.settings.BACKEND_API_TOKEN}"
        timeout = httpx.Timeout(10.0, connect=5.0)
        try:
            async with httpx.AsyncClient(
                base_url=self.settings.BACKEND_API_BASE_URL or "",
                headers=headers,
                timeout=timeout,
            ) as client:
                resp = await client.post(url, json=payload)
                if resp.status_code in (200, 201):
                    data = resp.json()
                    return data.get("code") == 2000 or data.get("success", False) or resp.status_code == 200
                logger.warning("dispatcher.callback_bad_status",
                               status=resp.status_code, body=resp.text[:500])
                return False
        except Exception as e:
            logger.error("dispatcher.callback_exception", error=str(e))
            return False