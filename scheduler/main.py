"""
taurus-scheduler main entry point:
  Starts APScheduler engine + FastAPI health check
Usage:
    python -m scheduler.main
    or: scheduler (after installing via poetry scripts)
"""
from __future__ import annotations

import threading
import sys

import uvicorn

from .config import get_settings
from .engine import ScriptTaskScheduler
from .health_api import app as health_app, register_engine

import structlog

logger = structlog.get_logger()


def main() -> int:
    settings = get_settings()
    logger.info("bootstrap", instance_id=settings.SCHEDULER_INSTANCE_ID,
                db_host=settings.DB_HOST, db_name=settings.DB_NAME)

    engine = ScriptTaskScheduler()
    engine.start()
    register_engine(engine)

    # Start health check HTTP service (separate thread)
    def _run_api():
        uvicorn.run(
            health_app,
            host=settings.HEALTH_HOST,
            port=settings.HEALTH_PORT,
            log_level="warning",
            access_log=False,
        )

    threading.Thread(target=_run_api, name="health-api", daemon=True).start()
    logger.info("health_api.started", host=settings.HEALTH_HOST, port=settings.HEALTH_PORT)

    try:
        engine.wait()
    finally:
        engine.shutdown()

    return 0


if __name__ == "__main__":
    sys.exit(main())