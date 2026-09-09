# Taurus Scheduler

独立的定时任务调度服务（Python + APScheduler + FastAPI），从主后端应用中解耦：它读取共享 MySQL 数据库中的 `ScriptTask`，在多副本间选举 Leader，并把到期任务推送到 Redis 队列，交由下游的 `Scheduler Worker` 消费执行。

> English version: [README.md](README.md)

## 在 Taurus Stack 中的角色

定时任务执行链路：

```
taurus-web (Schedule 页)
   │  写 ScriptTask
   ▼
taurus-backend ──写入──► MySQL（taurus_backend 共享库）
                               │ 只读轮询
                               ▼
                          taurus-scheduler（本服务）
                               │ 选举 Leader + 推送到期任务
                               ▼
                             Redis 队列（DB=2）
                               │ 消费
                               ▼
              backend run_scheduler_worker ──gRPC+mTLS──► taurus-executor
                               │ （队列不可达？HTTP 兜底回调）
                               ▼
                       backend /api/taurus/script_task/<id>/execute/
```

Scheduler **不执行任务本身，也不使用 gRPC/mTLS**——它只负责**派发**；真正的执行由 `Scheduler Worker`（taurus-backend 内的 management command）完成。通信安全细节见根仓库 `docs/developer-guide.md` 的 scheduler 章节。

## 功能特性

- **Leader 选举**：多个 scheduler 副本通过 Redis 分布式锁协调，仅 Leader 派发任务（HA）。
- **任务发现**：从共享的 `taurus_backend` 数据库（只读）轮询 `ScriptTask`，用 APScheduler 调度到期任务。
- **推送到队列**：将到期任务推送到 Redis 队列（DB=2）供 Worker 消费。
- **去重**：基于 Redis 的去重键，防止在窗口期内重复派发。
- **兜底回调**：队列消费者不可用时，回退为向 backend 发 HTTP 回调（`BACKEND_API_BASE_URL`）。
- **健康检查 API**：内置 FastAPI 端点（`/health`，端口 9101）。

## 快速开始

### 前置要求

- Python 3.12.x（通过 conda 环境 `taurus` 管理）
- Poetry
- MySQL 8+（与 taurus-backend 同一个 `taurus_backend` 数据库）
- Redis 7+（专用 DB=2）

### 安装

```bash
conda activate taurus
cd taurus-scheduler
poetry install
cp .env.example .env    # 按需修改下面配置
```

### 配置（`.env`）

| 变量                              | 说明                                            | 默认示例                                  |
| --------------------------------- | ---------------------------------------------- | ---------------------------------------- |
| `SCHEDULER_INSTANCE_ID`           | 实例唯一标识（用于 Leader 选举）                  | `scheduler-01`                           |
| `TIMEZONE`                        | 时区                                            | `Asia/Shanghai`                          |
| `DB_HOST` / `DB_PORT` / `DB_USER` / `DB_PASSWORD` / `DB_NAME` / `DB_TABLE_PREFIX` | 只读访问与 backend 相同的库 | `taurus_backend` / 前缀 `taurus_` |
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_PASSWORD` / `REDIS_DB` | Redis（必须 **DB=2**） | DB=2 |
| `REDIS_QUEUE_KEY`                 | 任务队列 Key                                     | `taurus:scheduler:queue:script_task`     |
| `REDIS_LOCK_PREFIX`               | Leader 锁前缀                                   | `taurus:scheduler:lock:`                 |
| `REDIS_DEDUP_PREFIX`              | 去重前缀                                         | `taurus:scheduler:dedup:`                |
| `BACKEND_API_BASE_URL`            | 可选的 HTTP 兜底回调地址                          | unset                                    |
| `HEALTH_HOST` / `HEALTH_PORT`     | 健康检查监听地址                                   | `0.0.0.0` / `9101`                       |

> ⚠️ **Redis DB 必须是 2**：backend 用 1（缓存）、auth 用 1（票据）；DB 2 为 scheduler 专属。

### 运行

```bash
conda activate taurus
cd taurus-scheduler
export PYTHONPATH=$PWD
python -m scheduler.main
```

健康检查：`curl http://localhost:9101/health`

### 配套：Scheduler Worker

任务推入队列后，由 taurus-backend 内的 worker 消费并执行：

```bash
cd taurus-backend
poetry run python manage.py run_scheduler_worker --workers 4
```

## 目录结构

```
taurus-scheduler/
├── scheduler/
│   ├── main.py            # 入口（调度引擎 + 健康检查 API）
│   ├── engine.py          # APScheduler 引擎 + Leader 选举
│   ├── dispatcher.py      # 派发任务到 Redis 队列
│   ├── lock.py            # Redis 分布式锁
│   ├── store.py           # MySQL 只读（轮询 ScriptTask）
│   ├── config.py          # pydantic-settings 配置
│   ├── health_api.py      # FastAPI 健康检查端点
│   └── logging_config.py
├── .env.example           # 配置模板
├── Dockerfile
└── pyproject.toml
```

## 技术栈

Python 3.12 + apscheduler 3.x + PyMySQL + redis + fastapi + uvicorn + prometheus-client + structlog。

## 测试

```bash
poetry run pytest    # pytest + pytest-asyncio
```

## 开发注意事项

- **Redis DB 必须是 2**——见上；用共享 DB 会与 backend/auth 冲突。
- Scheduler 对 MySQL 是**只读**访问（`ScriptTask` 表），执行记录由 Scheduler Worker（backend 进程内）回写。
- 多副本时 Leader 选举自动生效；单实例时 `SCHEDULER_INSTANCE_ID` 随便填。
- **不需要 mTLS**：本服务不经过 gRPC。详见根仓库 `docs/developer-guide.md` 的 scheduler 章节。

## 部署

```bash
docker build -t taurus-scheduler .
```

或通过根仓库的 compose 编排运行（见根目录 `docker-compose.yml` 与 `docker-compose.scheduler.yml`）。

## License

GNU Affero General Public License v3.0 — 见 [LICENSE](LICENSE)。

## 联系方式

- 邮箱：taurus-stack@outlook.com
- Issues：[GitHub Issues](https://github.com/taurus-ops/taurus-scheduler/issues)