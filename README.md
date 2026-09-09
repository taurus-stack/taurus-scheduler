# Taurus Scheduler

A standalone scheduled-task dispatching service (Python + APScheduler + FastAPI) that decouples from the main backend application: it reads `ScriptTask` from the shared MySQL database, elects a leader among replicas, and pushes due tasks onto a Redis queue for downstream `Scheduler Worker`s to consume and execute.

> 中文版见 [README.zh-CN.md](README.zh-CN.md)

## Role in Taurus Stack

The scheduled-task execution chain:

```
taurus-web (Schedule page)
   │  writes ScriptTask
   ▼
taurus-backend ──write──► MySQL (taurus_backend shared DB)
                                │ read-only poll
                                ▼
                           taurus-scheduler (this service)
                                │ leader election + push due tasks
                                ▼
                              Redis queue (DB=2)
                                │ consume
                                ▼
              backend run_scheduler_worker ──gRPC+mTLS──► taurus-executor
                                │ (queue unreachable? HTTP fallback)
                                ▼
                       backend /api/taurus/script_task/<id>/execute/
```

Scheduler does **not** execute tasks itself and does **not** use gRPC/mTLS. It only **dispatches**; the actual execution is done by the `Scheduler Worker` (a management command inside taurus-backend). See the scheduler section of `docs/developer-guide.md` for the full communication-security notes.

## Features

- **Leader Election**: multiple scheduler replicas coordinate via a Redis distributed lock; only the leader dispatches tasks (HA).
- **Task Discovery**: polls `ScriptTask` from the shared `taurus_backend` database (read-only) and schedules due jobs with APScheduler.
- **Dispatch to Queue**: pushes due tasks to a Redis queue (DB=2) for workers to pick up.
- **Deduplication**: Redis-based dedup keys prevent duplicate dispatch within a window.
- **Fallback Callback**: if the queue consumer is not ready, falls back to an HTTP callback to the backend (`BACKEND_API_BASE_URL`).
- **Health Check API**: a built-in FastAPI endpoint for liveness/readiness (`/health` on port 9101).

## Quick Start

### Prerequisites

- Python 3.12.x (managed via conda env `taurus`)
- Poetry
- MySQL 8+ (the same `taurus_backend` database used by taurus-backend)
- Redis 7+ (DB=2 dedicated to the scheduler)

### Installation

```bash
conda activate taurus
cd taurus-scheduler
poetry install
cp .env.example .env    # edit the settings below
```

### Configuration (`.env`)

| Variable                              | Description                                          | Default                                  |
| ------------------------------------- | ---------------------------------------------------- | ---------------------------------------- |
| `SCHEDULER_INSTANCE_ID`               | Unique instance id (used for leader election)        | `scheduler-01`                           |
| `TIMEZONE`                            | Timezone                                             | `Asia/Shanghai`                          |
| `DB_HOST` / `DB_PORT` / `DB_USER` / `DB_PASSWORD` / `DB_NAME` / `DB_TABLE_PREFIX` | Read-only access to the same DB as backend | `taurus_backend` / prefix `taurus_` |
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_PASSWORD` / `REDIS_DB` | Redis (must be **DB=2**)       | DB=2                                     |
| `REDIS_QUEUE_KEY`                     | Task queue key                                       | `taurus:scheduler:queue:script_task`     |
| `REDIS_LOCK_PREFIX`                   | Leader lock prefix                                   | `taurus:scheduler:lock:`                 |
| `REDIS_DEDUP_PREFIX`                  | Deduplication prefix                                 | `taurus:scheduler:dedup:`                |
| `BACKEND_API_BASE_URL`                | Optional HTTP fallback callback to backend           | unset                                    |
| `HEALTH_HOST` / `HEALTH_PORT`         | Health-check listen address                          | `0.0.0.0` / `9101`                       |

> ⚠️ **Redis DB must be 2**: backend uses DB 1 (cache), auth uses DB 1 (tickets); DB 2 is reserved exclusively for the scheduler.

### Run

```bash
conda activate taurus
cd taurus-scheduler
export PYTHONPATH=$PWD
python -m scheduler.main
```

Health check: `curl http://localhost:9101/health`

### Companion: Scheduler Worker

After tasks are pushed to the queue, the worker inside taurus-backend consumes and executes them:

```bash
cd taurus-backend
poetry run python manage.py run_scheduler_worker --workers 4
```

## Project Structure

```
taurus-scheduler/
├── scheduler/
│   ├── main.py            # Entry point (scheduling engine + health-check API)
│   ├── engine.py          # APScheduler engine + leader election
│   ├── dispatcher.py      # Dispatch tasks to the Redis queue
│   ├── lock.py            # Redis distributed lock
│   ├── store.py           # MySQL read-only (polls ScriptTask)
│   ├── config.py          # pydantic-settings configuration
│   ├── health_api.py      # FastAPI health-check endpoint
│   └── logging_config.py
├── .env.example           # config template
├── Dockerfile
└── pyproject.toml
```

## Tech Stack

Python 3.12 + apscheduler 3.x + PyMySQL + redis + fastapi + uvicorn + prometheus-client + structlog.

## Tests

```bash
poetry run pytest    # pytest + pytest-asyncio
```

## Development Notes

- **Redis DB must be 2** — see above; using a shared DB would conflict with backend/auth.
- Scheduler accesses MySQL **read-only** (the `ScriptTask` table); execution records are written back by the Scheduler Worker (inside the backend process).
- With multiple scheduler replicas, leader election works automatically; with a single instance, `SCHEDULER_INSTANCE_ID` can be any value.
- **No mTLS needed**: this service does not go through gRPC. See the scheduler section of `docs/developer-guide.md` for details.

## Deployment

Build the container:

```bash
docker build -t taurus-scheduler .
```

Or run via the repo-level compose (see the root `docker-compose.yml` and `docker-compose.scheduler.yml`).

## License

GNU Affero General Public License v3.0 — see [LICENSE](LICENSE).

## Contact

- Email: taurus-stack@outlook.com
- Issues: [GitHub Issues](https://github.com/taurus-ops/taurus-scheduler/issues)