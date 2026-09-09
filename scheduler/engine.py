"""
Taurus Scheduler Engine - Core
Based on APScheduler, combined with:
  - Custom Job synchronizer: syncs ScriptTask from DB every N seconds
  - Active-standby election + Leader lease renewal
  - Miss-fire compensation on startup
  - Task dispatch via TaskDispatcher when due
"""
from __future__ import annotations

import asyncio
import signal
import threading
import time
from datetime import datetime, timedelta
from typing import Optional

import pytz
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

import redis

from .config import get_settings
from .dispatcher import TaskDispatcher
from .lock import LeaderElector
from .logging_config import setup_logging
from .store import ScriptTaskRow, WorkflowRow, ScheduleRow, ScheduleType, TaskStore, TaskStatus

import structlog

logger = structlog.get_logger()


class ScriptTaskScheduler:
    """Core scheduler: translates ScriptTask from DB into APScheduler Jobs"""

    def __init__(self):
        setup_logging()
        self.settings = get_settings()
        self.tz = pytz.timezone(self.settings.TIMEZONE)

        # DB
        self.store = TaskStore()

        # Redis
        # M1.9 — CE standalone 允许 Redis 缺失：不建连接、Dispatcher 降级为只走 HTTP 回调
        self.redis: redis.Redis | None = None
        if self.settings.is_ha_cluster:
            self.redis = redis.Redis.from_url(
                self.settings.redis_dsn,
                decode_responses=False,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            try:
                self.redis.ping()
            except Exception as e:  # noqa: BLE001
                self._say(f"⚠  Redis 不可用（ha_cluster 模式启动失败）: {type(e).__name__}: {str(e)[:120]}")
                raise
        else:
            self._say("ℹ  Deployment mode = standalone (社区版 CE 单实例非 HA 降级)：Redis 可选，Dispatcher 将直接通过 BACKEND_API 触发")

        # Leader election + dispatcher
        self.leader = LeaderElector(self.redis)
        self.dispatcher = TaskDispatcher(self.redis, self.store)

        # APScheduler
        executors = {"default": ThreadPoolExecutor(max_workers=8)}
        job_defaults = {
            "coalesce": True,       # Coalesce multiple missed fires into one
            "max_instances": 1,     # Only one instance of the same task at a time
            "misfire_grace_time": self.settings.SCHEDULER_MISFIRE_GRACE_SECONDS,
        }
        self.scheduler = BackgroundScheduler(
            timezone=self.tz,
            executors=executors,
            job_defaults=job_defaults,
        )

        self._running = False
        self._sync_thread: Optional[threading.Thread] = None
        self._leader_thread: Optional[threading.Thread] = None
        self._last_sync_version: dict[int, str] = {}

    # ---------------- Human-readable stdout messages (independent of structlog JSON parsing) ----------------
    def _say(self, text: str) -> None:
        ts = datetime.now(self.tz).strftime("%m-%d %H:%M:%S")
        try:
            print(f"[taurus-scheduler {ts}] {text}", flush=True)
        except Exception:
            pass

    @staticmethod
    def _fmt_delta(seconds: int) -> str:
        seconds = int(max(0, seconds))
        if seconds < 60:
            return f"{seconds}s"
        if seconds < 3600:
            return f"{seconds // 60}m{seconds % 60}s"
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h}h{m}m"

    # ===================== Lifecycle =====================
    def start(self) -> None:
        logger.info("scheduler.starting", instance_id=self.settings.SCHEDULER_INSTANCE_ID)
        self._running = True

        # Start with an empty scheduler, no jobs scheduled, add jobs only after becoming Leader
        self.scheduler.start(paused=True)
        logger.info("apscheduler.started.paused", state="paused until elected leader")

        # Start job sync thread (separate thread, runs regardless of leader status,
        # so that when leadership changes the instance can immediately take over with latest tasks
        self._sync_thread = threading.Thread(target=self._sync_loop, name="job-sync", daemon=True)
        self._sync_thread.start()

        # Start active-standby election + lease renewal thread
        self._leader_thread = threading.Thread(target=self._leader_loop, name="leader-election", daemon=True)
        self._leader_thread.start()

        # Install signal handlers: graceful exit on SIGINT/SIGTERM
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._graceful_shutdown)

        logger.info("scheduler.started", instance_id=self.settings.SCHEDULER_INSTANCE_ID)

    def wait(self) -> None:
        """Block and wait until a signal is received"""
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        if not self._running:
            return
        logger.info("scheduler.shutting_down")
        self._running = False
        self.leader.step_down()
        try:
            self.scheduler.shutdown(wait=False)
        except Exception:
            pass
        try:
            self.redis.close()
        except Exception:
            pass
        try:
            self.store.close()
        except Exception:
            pass
        logger.info("scheduler.stopped")

    def _graceful_shutdown(self, signum, frame) -> None:
        logger.info("scheduler.signal_received", signal=signum)
        self._running = False

    # ===================== Leader Loop =====================
    def _leader_loop(self) -> None:
        """Every 5 seconds try to become / stay as Leader"""
        while self._running:
            try:
                if self.leader.is_leader:
                    # Renew lease
                    ok = self.leader.keepalive()
                    if not ok:
                        self._yield_leadership()
                else:
                    # Run for election
                    if self.leader.try_become_leader():
                        self._assume_leadership()
                time.sleep(5)
            except Exception as e:
                logger.exception("leader.loop_error", error=str(e))
                time.sleep(5)

    def _assume_leadership(self) -> None:
        """Become Leader: immediately compensate missed jobs + resume APScheduler"""
        logger.info("leader.assume.roles")
        self._say("🗝  Elected as Leader, starting to take over scheduling...")
        try:
            self._sync_jobs_from_db(force=True)
            self._compensate_missed_jobs()
            self._register_maintenance_jobs()
            # Resume scheduling
            self.scheduler.resume()
            logger.info("apscheduler.resumed")
            total_jobs = len(self.scheduler.get_jobs())
            self._say(f"▶  APScheduler resumed, currently managing {total_jobs} scheduled tasks")
        except Exception as e:
            logger.exception("leader.assume_failed", error=str(e))
            self._say(f"✗ Failed to become Leader: {type(e).__name__}: {str(e)[:120]}")

    def _yield_leadership(self) -> None:
        """Lost Leader: pause scheduling, but do not clear Jobs (takes effect immediately when regained next time)"""
        logger.warning("leader.yield")
        try:
            self.scheduler.pause()
        except Exception as e:
            logger.warning("apscheduler.pause_failed", error=str(e))

    # ===================== Task Sync Loop =====================
    def _sync_loop(self) -> None:
        interval = max(2, self.settings.SCHEDULER_JOB_SYNC_INTERVAL)
        while self._running:
            start = time.time()
            try:
                self._sync_jobs_from_db(force=False)
            except Exception as e:
                logger.exception("sync.loop_error", error=str(e))
            elapsed = time.time() - start
            sleep_for = max(0.1, interval - elapsed)
            time.sleep(sleep_for)

    def _sync_jobs_from_db(self, force: bool) -> None:
        """
        Load all enabled ScriptTask + has_schedule Workflow from DB,
        compare with current APScheduler Jobs:
          - DB has, Scheduler does not → add
          - DB has, Scheduler has but config changed → update
          - DB does not have, Scheduler has → delete
        """
        try:
            tasks = self.store.list_enabled_script_tasks()
        except Exception as e:
            logger.error("sync.load_script_tasks_failed", error=str(e))
            tasks = []

        try:
            workflows = self.store.list_enabled_workflows()
        except Exception as e:
            logger.error("sync.load_workflows_failed", error=str(e))
            workflows = []

        try:
            schedules = self.store.list_enabled_schedules()
        except Exception as e:
            logger.error("sync.load_schedules_failed", error=str(e))
            schedules = []

        # ScriptTask jobs
        db_task_ids = {t.id for t in tasks}
        current_task_job_ids = {j.id.split(":", 1)[1] for j in self.scheduler.get_jobs()
                                if j.id.startswith("script_task:")}
        current_task_job_ids = {int(x) for x in current_task_job_ids if x.isdigit()}

        for missing in current_task_job_ids - db_task_ids:
            job_id = f"script_task:{missing}"
            try:
                self.scheduler.remove_job(job_id)
                logger.info("sync.job_removed", job_id=job_id)
            except JobLookupError:
                pass
            self._last_sync_version.pop(("script_task", missing), None)

        for task in tasks:
            version = self._job_version(task)
            key = ("script_task", task.id)
            if force or self._last_sync_version.get(key) != version:
                self._upsert_job(task)
                self._last_sync_version[key] = version
                logger.debug("sync.job_upserted", task_id=task.id,
                             added=task.id not in current_task_job_ids,
                             schedule_type=task.schedule_type.value)

        # Workflow jobs
        db_wf_ids = {w.id for w in workflows}
        current_wf_job_ids = {j.id.split(":", 1)[1] for j in self.scheduler.get_jobs()
                               if j.id.startswith("workflow:")}
        current_wf_job_ids = {int(x) for x in current_wf_job_ids if x.isdigit()}

        for missing in current_wf_job_ids - db_wf_ids:
            job_id = f"workflow:{missing}"
            try:
                self.scheduler.remove_job(job_id)
                logger.info("sync.workflow_job_removed", job_id=job_id)
            except JobLookupError:
                pass
            self._last_sync_version.pop(("workflow", missing), None)

        for wf in workflows:
            version = self._job_version(wf)
            key = ("workflow", wf.id)
            if force or self._last_sync_version.get(key) != version:
                self._upsert_job(wf)
                self._last_sync_version[key] = version
                logger.debug("sync.workflow_upserted", workflow_id=wf.id,
                             added=wf.id not in current_wf_job_ids,
                             schedule_type=wf.schedule_type.value)

        # Legacy Schedule jobs
        db_sch_ids = {s.id for s in schedules}
        current_sch_job_ids = {j.id.split(":", 1)[1] for j in self.scheduler.get_jobs()
                               if j.id.startswith("schedule:")}
        current_sch_job_ids = {int(x) for x in current_sch_job_ids if x.isdigit()}

        for missing in current_sch_job_ids - db_sch_ids:
            job_id = f"schedule:{missing}"
            try:
                self.scheduler.remove_job(job_id)
                logger.info("sync.schedule_job_removed", job_id=job_id)
            except JobLookupError:
                pass
            self._last_sync_version.pop(("schedule", missing), None)

        for sch in schedules:
            version = self._job_version(sch)
            key = ("schedule", sch.id)
            if force or self._last_sync_version.get(key) != version:
                self._upsert_job(sch)
                self._last_sync_version[key] = version
                logger.debug("sync.schedule_upserted", schedule_id=sch.id,
                             added=sch.id not in current_sch_job_ids,
                             schedule_type=sch.schedule_type.value)

    @staticmethod
    def _job_version(t) -> str:
        """Used to determine if task schedule config has changed; if changed, Trigger must be rebuilt"""
        if isinstance(t, WorkflowRow):
            parts = [
                str(t.schedule_type.value),
                str(t.cron_expression or ""),
                str(t.interval_seconds or ""),
                t.run_once_at.isoformat() if isinstance(t.run_once_at, datetime) else "",
                str(t.schedule_enabled),
                t.update_datetime.isoformat() if isinstance(t.update_datetime, datetime) else "",
            ]
        elif isinstance(t, ScheduleRow):
            parts = [
                str(t.schedule_type.value),
                str(t.cron_expression or ""),
                str(t.interval_seconds or ""),
                t.run_once_at.isoformat() if isinstance(t.run_once_at, datetime) else "",
                str(t.status),
                t.update_datetime.isoformat() if isinstance(t.update_datetime, datetime) else "",
            ]
        else:
            parts = [
                str(t.schedule_type.value),
                str(t.cron_expression or ""),
                str(t.interval_seconds or ""),
                t.run_once_at.isoformat() if isinstance(t.run_once_at, datetime) else "",
                str(t.enabled),
                t.update_datetime.isoformat() if isinstance(t.update_datetime, datetime) else "",
            ]
        return "|".join(parts)

    def _upsert_job(self, task) -> None:
        """Create APScheduler Trigger based on ScriptTask / Workflow / Schedule configuration"""
        is_workflow = isinstance(task, WorkflowRow)
        is_schedule = isinstance(task, ScheduleRow)
        prefix = "workflow" if is_workflow else ("schedule" if is_schedule else "script_task")
        job_id = f"{prefix}:{task.id}"
        trigger = self._make_trigger(task)
        if trigger is None:
            logger.warning("sync.skip_invalid_task", task_id=task.id,
                           reason="no valid trigger", task_type=prefix)
            return

        kwargs = dict(
            func=self._job_runner,
            trigger=trigger,
            id=job_id,
            name=task.name,
            args=[task.id, is_workflow, is_schedule],
            replace_existing=True,
        )

        if isinstance(trigger, DateTrigger):
            now = datetime.now(self.tz)
            if trigger.run_date < now:
                logger.info("sync.once_task_already_past", task_id=task.id,
                            run_date=trigger.run_date.isoformat())
                try:
                    self.scheduler.remove_job(job_id)
                except JobLookupError:
                    pass
                if is_workflow:
                    self.store.update_workflow_next_exec_time(task.id, None)
                    self.store.disable_workflow_schedule(task.id)
                elif is_schedule:
                    self.store.update_schedule_after_execution(task.id, None)
                    self.store.disable_schedule_once(task.id)
                else:
                    self.store.update_task_next_exec_time(task.id, None)
                    self.store.disable_task(task.id)
                logger.warning("sync.once_task_disabled_past_due", task_id=task.id,
                               run_date=trigger.run_date.isoformat())
                return

        self.scheduler.add_job(**kwargs)

        # Calculate and write back next execution time
        next_fire_dt: Optional[datetime] = None
        try:
            next_fire = self.scheduler.get_job(job_id)
            if next_fire and next_fire.next_run_time:
                nxt = next_fire.next_run_time
                next_fire_dt = nxt
                if hasattr(nxt, "astimezone"):
                    nxt = nxt.astimezone(self.tz).replace(tzinfo=None)
                if is_workflow:
                    self.store.update_workflow_next_exec_time(task.id, nxt)
                elif is_schedule:
                    self.store.update_schedule_after_execution(task.id, nxt)
                else:
                    self.store.update_task_next_exec_time(task.id, nxt)
        except Exception as e:
            logger.debug("sync.next_exec_time_error", task_id=task.id, error=str(e))

        # Human-readable stdout message
        try:
            st = task.schedule_type.value
            if st == "cron":
                expr = task.cron_expression or ""
                hint = f'cron="{expr}"'
            elif st == "interval":
                hint = f"interval={task.interval_seconds}s"
            else:
                run_at = ""
                if task.run_once_at:
                    run_at = task.run_once_at.strftime("%m-%d %H:%M:%S") if isinstance(task.run_once_at, datetime) else str(task.run_once_at)
                hint = f"run_once_at={run_at}"
            wait_info = ""
            if next_fire_dt is not None:
                now = datetime.now(self.tz)
                try:
                    aware = next_fire_dt
                    if not hasattr(aware, "tzinfo") or aware.tzinfo is None:
                        aware = self.tz.localize(next_fire_dt)
                    delta_sec = int((aware - now).total_seconds())
                    wait_info = f"  → next: {aware.strftime('%m-%d %H:%M:%S')} (in {self._fmt_delta(delta_sec)})"
                except Exception:
                    pass
            type_label = "Workflow" if is_workflow else "ScriptTask"
            self._say(
                f"✅ Registered {type_label} id={task.id} name={task.name[:40]} "
                f"{hint}{wait_info}"
            )
        except Exception:
            pass

    def _make_trigger(self, task):
        try:
            if task.schedule_type == ScheduleType.CRON:
                expr = (task.cron_expression or "").strip()
                if not expr:
                    return None
                parts = expr.split()
                if len(parts) < 5:
                    return None

                # Frontend dropdown and Quartz format are both 6 fields (sec min hr day mon wk) or 7 fields (+ year)
                # APScheduler CronTrigger parameter order: year, month, day, week, day_of_week, hour, minute, second
                # Here we split correctly based on parts length:
                #   len=5: Traditional crontab → min hr day mon wk (second defaults to 0)
                #   len=6: Quartz     → sec min hr day mon wk
                #   len=7: Quartz+yr  → sec min hr day mon wk year
                second = 0
                if len(parts) == 5:
                    minute, hour, day, month, day_of_week = parts[0], parts[1], parts[2], parts[3], parts[4]
                else:
                    second = parts[0]
                    minute, hour, day, month, day_of_week = parts[1], parts[2], parts[3], parts[4], parts[5]

                # Quartz's "?" means "unspecified", should pass APScheduler default None,
                # passing "?" string will be judged invalid day_of_week / day by APScheduler, causing tasks to never fire
                if day_of_week in ("?",):
                    day_of_week = None
                elif day_of_week == "*":
                    day_of_week = "*"
                else:
                    # Quartz 1=Sun..7=Sat → APScheduler 0=Mon..6=Sun
                    mapping = {"1": "6", "2": "0", "3": "1", "4": "2",
                               "5": "3", "6": "4", "7": "5",
                               "SUN": "6", "MON": "0", "TUE": "1", "WED": "2",
                               "THU": "3", "FRI": "4", "SAT": "5"}
                    up = day_of_week.upper()
                    if up in mapping:
                        day_of_week = mapping[up]
                if day in ("?",):
                    day = None
                return CronTrigger(
                    second=second,
                    minute=minute, hour=hour, day=day,
                    month=month, day_of_week=day_of_week,
                    timezone=self.tz,
                )

            elif task.schedule_type == ScheduleType.INTERVAL:
                secs = task.interval_seconds or 0
                if secs <= 0:
                    return None
                return IntervalTrigger(seconds=secs, timezone=self.tz)

            elif task.schedule_type == ScheduleType.ONCE:
                if not task.run_once_at:
                    return None
                dt = self.tz.localize(task.run_once_at) if task.run_once_at.tzinfo is None else task.run_once_at
                return DateTrigger(run_date=dt, timezone=self.tz)
        except Exception as e:
            logger.warning("scheduler.make_trigger_failed", task_id=task.id, error=str(e))
            return None
        return None

    # ===================== Job Execution Entry (APScheduler callback) =====================
    def _job_runner(self, task_id: int, is_workflow: bool = False, is_schedule: bool = False) -> None:
        """Called by APScheduler in worker thread when job is due"""
        if not self.leader.is_leader:
            logger.info("runner.skip_not_leader", task_id=task_id, is_workflow=is_workflow, is_schedule=is_schedule)
            self._say(f"⏰  task_id={task_id} triggered on schedule, but not Leader, skipping")
            return

        if is_workflow:
            return self._run_workflow_job(task_id)
        elif is_schedule:
            return self._run_schedule_job(task_id)
        else:
            return self._run_script_task_job(task_id)

    def _run_workflow_job(self, workflow_id: int) -> None:
        wf = self.store.get_workflow(workflow_id)
        if wf is None:
            logger.warning("runner.workflow_not_found", workflow_id=workflow_id)
            self._say(f"⚠  workflow_id={workflow_id} is due, but workflow does not exist in DB")
            try:
                self.scheduler.remove_job(f"workflow:{workflow_id}")
            except JobLookupError:
                pass
            return
        if not wf.schedule_enabled:
            logger.info("runner.workflow_disabled", workflow_id=workflow_id)
            self._say(f"⏹  workflow_id={workflow_id} ({wf.name[:40]}) is disabled, skipping")
            return

        wf_status = self.store.get_workflow_status(workflow_id)
        if wf_status == 2:
            logger.info("runner.workflow_pending_approval", workflow_id=workflow_id)
            self._say(f"⏹  workflow_id={workflow_id} workflow is pending approval, skipping")
            return
        if wf_status in (1, 3):
            logger.info("runner.workflow_inactive", workflow_id=workflow_id, status=wf_status)
            self._say(f"⏹  workflow_id={workflow_id} workflow is disabled or archived, skipping")
            return

        self._say(f"⏰ Triggering Workflow id={workflow_id} name={wf.name[:40]}")
        scheduled_fire_time = datetime.now()

        dispatched = False
        try:
            loop = asyncio.new_event_loop()
            dispatched = loop.run_until_complete(
                self.dispatcher.dispatch_workflow(wf, scheduled_fire_time=scheduled_fire_time)
            )
            loop.close()
        except Exception as e:
            logger.exception("runner.workflow_execute_error", workflow_id=workflow_id, error=str(e))
            self._say(f"✗  workflow_id={workflow_id} dispatch exception: {type(e).__name__}: {str(e)[:120]}")
            return

        next_hint = ""
        try:
            job = self.scheduler.get_job(f"workflow:{workflow_id}")
            if job and job.next_run_time:
                nxt = job.next_run_time
                try:
                    aware = nxt
                    if not hasattr(aware, "tzinfo") or aware.tzinfo is None:
                        aware = self.tz.localize(aware)
                    delta_sec = int((aware - datetime.now(self.tz)).total_seconds())
                    next_hint = f"  next: {aware.strftime('%m-%d %H:%M:%S')} (in {self._fmt_delta(delta_sec)})"
                except Exception:
                    pass
                if hasattr(nxt, "astimezone"):
                    nxt = nxt.astimezone(self.tz).replace(tzinfo=None)
                self.store.update_workflow_next_exec_time(workflow_id, nxt)
        except Exception:
            pass

        if dispatched:
            self._say(f"✅ Workflow dispatched successfully id={workflow_id} →Redis queue{next_hint}")
        else:
            self._say(f"⚠  workflow_id={workflow_id} dispatcher returned False")

        if wf.schedule_type == ScheduleType.ONCE and dispatched:
            try:
                self.store.disable_workflow_schedule(workflow_id)
                self._say(f"⏹  workflow_id={workflow_id} is a one-time task, automatically disabled after dispatch")
            except Exception as e:
                logger.exception("runner.workflow_disable_once_failed", workflow_id=workflow_id, error=str(e))

    def _run_script_task_job(self, task_id: int) -> None:
        task = self.store.get_script_task(task_id)
        if task is None:
            logger.warning("runner.task_not_found", task_id=task_id)
            self._say(f"⚠  task_id={task_id} is due, but task does not exist in DB, removing corresponding APScheduler Job")
            try:
                self.scheduler.remove_job(f"script_task:{task_id}")
            except JobLookupError:
                pass
            return
        if not task.enabled:
            logger.info("runner.task_disabled", task_id=task_id)
            self._say(f"⏹  task_id={task_id} ({task.name[:40]}) is due, but disabled, skipping")
            return

        script_status = self.store.get_script_status(task.script_id)
        if script_status == 2:
            logger.info("runner.script_pending_approval", task_id=task_id, script_id=task.script_id)
            self._say(f"⏹  task_id={task_id} associated script is pending approval, skipping")
            return
        if script_status in (1, 3):
            logger.info("runner.script_inactive", task_id=task_id, script_id=task.script_id, status=script_status)
            self._say(f"⏹  task_id={task_id} associated script is disabled or archived, skipping")
            return

        self._say(f"⏰ Triggering task_id={task_id} name={task.name[:40]}")
        scheduled_fire_time = datetime.now()

        dispatched = False
        try:
            loop = asyncio.new_event_loop()
            dispatched = loop.run_until_complete(
                self.dispatcher.dispatch(task, scheduled_fire_time=scheduled_fire_time)
            )
            loop.close()
        except Exception as e:
            logger.exception("runner.execute_error", task_id=task.id, error=str(e))
            self._say(f"✗  task_id={task_id} dispatch exception: {type(e).__name__}: {str(e)[:120]}")
            return

        next_hint = ""
        try:
            job = self.scheduler.get_job(f"script_task:{task_id}")
            if job and job.next_run_time:
                nxt = job.next_run_time
                try:
                    aware = nxt
                    if not hasattr(aware, "tzinfo") or aware.tzinfo is None:
                        aware = self.tz.localize(aware)
                    delta_sec = int((aware - datetime.now(self.tz)).total_seconds())
                    next_hint = f"  next: {aware.strftime('%m-%d %H:%M:%S')} (in {self._fmt_delta(delta_sec)})"
                except Exception:
                    pass
                if hasattr(nxt, "astimezone"):
                    nxt = nxt.astimezone(self.tz).replace(tzinfo=None)
                self.store.update_task_next_exec_time(task.id, nxt)
        except Exception:
            pass

        if dispatched:
            self._say(f"✅ Dispatched successfully task_id={task_id} execution written to DB→Redis queue{next_hint}")
        else:
            self._say(f"⚠  task_id={task_id} dispatcher.dispatch returned False")

        if task.schedule_type == ScheduleType.ONCE and dispatched:
            try:
                self.store.disable_task(task.id)
                self._say(f"⏹  task_id={task_id} is a one-time task, automatically disabled after dispatch")
            except Exception as e:
                logger.exception("runner.disable_once_task_failed", task_id=task.id, error=str(e))

    def _run_schedule_job(self, schedule_id: int) -> None:
        sch = self.store.get_schedule(schedule_id)
        if sch is None:
            logger.warning("runner.schedule_not_found", schedule_id=schedule_id)
            self._say(f"⚠  schedule_id={schedule_id} is due, but does not exist in DB, removing APScheduler Job")
            try:
                self.scheduler.remove_job(f"schedule:{schedule_id}")
            except JobLookupError:
                pass
            return
        if not sch.enabled:
            logger.info("runner.schedule_disabled", schedule_id=schedule_id)
            self._say(f"⏹  schedule_id={schedule_id} ({sch.name[:40]}) is disabled, skipping")
            return

        self._say(f"⏰ Triggering Schedule id={schedule_id} name={sch.name[:40]} target={sch.target_type}")
        scheduled_fire_time = datetime.now()

        dispatched = False
        try:
            loop = asyncio.new_event_loop()
            dispatched = loop.run_until_complete(
                self.dispatcher.dispatch_schedule(sch, scheduled_fire_time=scheduled_fire_time)
            )
            loop.close()
        except Exception as e:
            logger.exception("runner.schedule_execute_error", schedule_id=schedule_id, error=str(e))
            self._say(f"✗  schedule_id={schedule_id} dispatch exception: {type(e).__name__}: {str(e)[:120]}")
            return

        next_hint = ""
        try:
            job = self.scheduler.get_job(f"schedule:{schedule_id}")
            if job and job.next_run_time:
                nxt = job.next_run_time
                try:
                    aware = nxt
                    if not hasattr(aware, "tzinfo") or aware.tzinfo is None:
                        aware = self.tz.localize(aware)
                    delta_sec = int((aware - datetime.now(self.tz)).total_seconds())
                    next_hint = f"  next: {aware.strftime('%m-%d %H:%M:%S')} (in {self._fmt_delta(delta_sec)})"
                except Exception:
                    pass
                if hasattr(nxt, "astimezone"):
                    nxt = nxt.astimezone(self.tz).replace(tzinfo=None)
                self.store.update_schedule_after_execution(schedule_id, nxt)
        except Exception:
            pass

        if dispatched:
            self._say(f"✅ Schedule dispatched id={schedule_id} →Redis queue{next_hint}")
        else:
            self._say(f"⚠  schedule_id={schedule_id} dispatcher returned False")

        if sch.schedule_type == ScheduleType.ONCE and dispatched:
            try:
                self.store.disable_schedule_once(schedule_id)
                self._say(f"⏹  schedule_id={schedule_id} is a one-time task, automatically disabled after dispatch")
            except Exception as e:
                logger.exception("runner.schedule_disable_once_failed", schedule_id=schedule_id, error=str(e))

    # ===================== Maintenance Jobs =====================
    def _run_maintenance_job(self, task_name: str, params: dict | None = None) -> None:
        """Called by APScheduler for internal maintenance jobs (heartbeat check, record cleanup)."""
        if not self.leader.is_leader:
            return
        try:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(
                self.dispatcher.dispatch_maintenance(task_name, params=params or {})
            )
            loop.close()
        except Exception as e:
            logger.exception("runner.maintenance_error", task_name=task_name, error=str(e))

    def _register_maintenance_jobs(self) -> None:
        """Register internal maintenance jobs when this instance becomes leader.
        - check_host_heartbeat_timeout: every 60 seconds
        - cleanup_old_heartbeat_records: daily at 02:30
        """
        try:
            self.scheduler.add_job(
                func=self._run_maintenance_job,
                trigger=IntervalTrigger(seconds=60, timezone=self.tz),
                id="maintenance:check_host_heartbeat_timeout",
                name="check_host_heartbeat_timeout",
                args=["check_host_heartbeat_timeout"],
                replace_existing=True,
            )
            self.scheduler.add_job(
                func=self._run_maintenance_job,
                trigger=CronTrigger(hour=2, minute=30, timezone=self.tz),
                id="maintenance:cleanup_old_heartbeat_records",
                name="cleanup_old_heartbeat_records",
                args=["cleanup_old_heartbeat_records", {"days": 30}],
                replace_existing=True,
            )
            self._say("🛠  Registered maintenance jobs: heartbeat check (60s), heartbeat cleanup (daily 02:30)")
        except Exception as e:
            logger.exception("maintenance.register_failed", error=str(e))

    # ===================== Startup Compensation =====================
    def _compensate_missed_jobs(self) -> None:
        """Compensate missed tasks when becoming Leader:
        next_exec_time within [now - grace, now] and last_exec_time < next_exec_time
        """
        grace = self.settings.SCHEDULER_MISFIRE_GRACE_SECONDS
        try:
            missed = self.store.list_missed_script_tasks(grace_seconds=grace)
        except Exception as e:
            logger.error("compensate.load_failed", error=str(e))
            missed = []

        try:
            missed_wf = self.store.list_missed_workflows(grace_seconds=grace)
        except Exception as e:
            logger.error("compensate.wf_load_failed", error=str(e))
            missed_wf = []

        try:
            missed_sch = self.store.list_missed_schedules(grace_seconds=grace)
        except Exception as e:
            logger.error("compensate.sch_load_failed", error=str(e))
            missed_sch = []

        if not missed and not missed_wf and not missed_sch:
            logger.info("compensate.no_missed")
            return

        logger.warning("compensate.found_missed",
                       script_task_count=len(missed), workflow_count=len(missed_wf),
                       schedule_count=len(missed_sch))
        self._say(f"🔁 Startup compensation: found {len(missed)} missed ScriptTask + "
                  f"{len(missed_wf)} missed Workflow + {len(missed_sch)} missed Schedule (grace={grace}s)")

        loop = asyncio.new_event_loop()
        success_n = 0
        fail_n = 0
        try:
            for task in missed:
                try:
                    ok = loop.run_until_complete(
                        self.dispatcher.dispatch(
                            task,
                            scheduled_fire_time=task.next_exec_time,
                            trigger_type="compensate",
                        )
                    )
                    if ok:
                        success_n += 1
                    else:
                        fail_n += 1
                        self._say(f"  ✗ Compensation failed task_id={task.id} dispatcher returned False")
                except Exception as e:
                    fail_n += 1
                    logger.exception("compensate.task_failed", task_id=task.id, error=str(e))
                    self._say(f"  ✗ Compensation exception task_id={task.id}: {type(e).__name__}: {str(e)[:80]}")

            for wf in missed_wf:
                try:
                    ok = loop.run_until_complete(
                        self.dispatcher.dispatch_workflow(
                            wf,
                            scheduled_fire_time=wf.next_exec_time,
                            trigger_type="compensate",
                        )
                    )
                    if ok:
                        success_n += 1
                    else:
                        fail_n += 1
                        self._say(f"  ✗ Compensation failed workflow_id={wf.id} dispatcher returned False")
                except Exception as e:
                    fail_n += 1
                    logger.exception("compensate.wf_failed", workflow_id=wf.id, error=str(e))
                    self._say(f"  ✗ Compensation exception workflow_id={wf.id}: {type(e).__name__}: {str(e)[:80]}")

            for sch in missed_sch:
                try:
                    ok = loop.run_until_complete(
                        self.dispatcher.dispatch_schedule(
                            sch,
                            scheduled_fire_time=sch.next_run_time,
                            trigger_type="compensate",
                        )
                    )
                    if ok:
                        success_n += 1
                    else:
                        fail_n += 1
                        self._say(f"  ✗ Compensation failed schedule_id={sch.id} dispatcher returned False")
                except Exception as e:
                    fail_n += 1
                    logger.exception("compensate.sch_failed", schedule_id=sch.id, error=str(e))
                    self._say(f"  ✗ Compensation exception schedule_id={sch.id}: {type(e).__name__}: {str(e)[:80]}")
        finally:
            loop.close()
        self._say(f"  ✅ Compensation complete: {success_n} succeeded {fail_n} failed")