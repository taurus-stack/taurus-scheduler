"""
Taurus Scheduler - Health check & monitoring endpoints
FastAPI exposes /health, /ready, /metrics
Started in a separate thread, does not affect APScheduler core scheduling
"""
from __future__ import annotations

import threading
import time
from typing import Any

from fastapi import FastAPI, Response
from fastapi.responses import PlainTextResponse
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST

import structlog

logger = structlog.get_logger()


app = FastAPI(title="Taurus Scheduler Health", version="1.0.0")

# Prometheus Metrics
METRIC_LEADER_STATUS = Gauge("taurus_scheduler_leader_status", "1=current instance is Leader 0=not Leader")
METRIC_JOBS_TOTAL = Gauge("taurus_scheduler_jobs_loaded_total", "Number of currently loaded scheduled jobs")
METRIC_DISPATCH_TOTAL = Counter("taurus_scheduler_dispatch_total", "Total number of task dispatches", ["result"])
METRIC_SYNC_LOOP_LAST_OK = Gauge("taurus_scheduler_sync_last_success_timestamp_seconds", "Timestamp of last successful task sync")


def register_engine(engine) -> None:
    """Bind the scheduler engine instance to FastAPI routes for health check status reads"""
    app.state.engine = engine

    # Background metrics refresh (every 15s)
    def _refresh():
        while True:
            try:
                METRIC_LEADER_STATUS.set(1 if engine.leader.is_leader else 0)
                jobs = engine.scheduler.get_jobs() if engine.scheduler.running else []
                METRIC_JOBS_TOTAL.set(len(jobs))
            except Exception:
                pass
            time.sleep(15)

    threading.Thread(target=_refresh, name="metrics-refresh", daemon=True).start()


@app.get("/health")
def health() -> dict[str, Any]:
    engine = getattr(app.state, "engine", None)
    engine_ok = engine is not None and engine._running
    leader = engine.leader.is_leader if engine_ok else False
    try:
        if engine:
            engine.redis.ping()
            redis_ok = True
        else:
            redis_ok = False
    except Exception:
        redis_ok = False
    status = "ok" if (engine_ok and redis_ok) else "degraded"
    return {
        "status": status,
        "service": "taurus-scheduler",
        "instance": engine.settings.SCHEDULER_INSTANCE_ID if engine else "unknown",
        "leader": leader,
        "components": {
            "engine": engine_ok,
            "redis": redis_ok,
        },
        "timestamp": time.time(),
    }


@app.get("/ready")
def ready() -> Response:
    """Readiness probe: both Leader/Follower are considered ready (Follower just stands by)"""
    h = health()
    code = 200 if h["components"]["engine"] and h["components"]["redis"] else 503
    return Response(status_code=code, media_type="application/json",
                    content=str(h).encode("utf-8") if False else None)


@app.get("/metrics", response_class=PlainTextResponse)
def metrics() -> Response:
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)


@app.get("/status")
def status_detail() -> dict[str, Any]:
    engine = getattr(app.state, "engine", None)
    if engine is None:
        return {"error": "engine not attached"}
    jobs = []
    for j in engine.scheduler.get_jobs():
        jobs.append({
            "id": j.id,
            "name": j.name,
            "trigger": str(j.trigger),
            "next_run_time": j.next_run_time.isoformat() if j.next_run_time else None,
        })
    return {
        "instance": engine.settings.SCHEDULER_INSTANCE_ID,
        "is_leader": engine.leader.is_leader,
        "apscheduler_running": engine.scheduler.running,
        "jobs": jobs,
    }