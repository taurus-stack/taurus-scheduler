"""
Taurus Scheduler configuration module
Loads configuration from environment variables using pydantic-settings,
fully decoupled from the main Django application
"""
from functools import lru_cache
from typing import Literal, Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

# 调度服务部署模式（全功能版本不再按 edition 区分，由部署方自行选择）
#   · standalone  ：单实例模式（默认，零配置）——无分布式锁、无队列、始终认为自己是
#                    Leader，直接 DB 轮询 + BACKEND_API 触发执行；单机故障则停机直到恢复。
#   · ha_cluster  ：Redis 主备选举 + 队列 + 幂等去重 + HTTP 兜底回调（多副本高可用）。
DeploymentMode = Literal["standalone", "ha_cluster"]


def _resolve_deployment_mode(user_value: Optional[str]) -> DeploymentMode:
    if user_value and user_value.lower() in ("standalone", "ha_cluster"):
        return user_value.lower()  # type: ignore[return-value]
    # 默认单实例；需要 HA 时显式设置 SCHEDULER_DEPLOYMENT_MODE=ha_cluster
    return "standalone"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Service identity
    APP_NAME: str = "taurus-scheduler"
    LOG_LEVEL: str = "INFO"

    # Database (shares the same MySQL with taurus-backend, read-only access to ScriptTask/Schedule tables)
    DB_HOST: str = "127.0.0.1"
    DB_PORT: int = 3306
    DB_USER: str = "root"
    DB_PASSWORD: str = "taurus"
    DB_NAME: str = "taurus_backend"
    DB_TABLE_PREFIX: str = "taurus_"
    DB_CHARSET: str = "utf8mb4"

    # Redis (distributed lock + task queue + idempotent deduplication)
    REDIS_HOST: str = "127.0.0.1"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: str = "AOADMIN3"
    REDIS_DB: int = 2
    REDIS_URL: Optional[str] = None
    REDIS_LOCK_PREFIX: str = "taurus:scheduler:lock:"
    REDIS_QUEUE_KEY: str = "taurus:scheduler:queue:script_task"
    REDIS_WORKFLOW_QUEUE_KEY: str = "taurus:scheduler:queue:workflow"
    REDIS_MAINTENANCE_QUEUE_KEY: str = "taurus:scheduler:queue:maintenance"
    REDIS_DEDUP_PREFIX: str = "taurus:scheduler:dedup:"

    # Scheduler configuration
    SCHEDULER_INSTANCE_ID: str = "scheduler-01"
    SCHEDULER_LEADER_LOCK_KEY: str = "taurus:scheduler:leader"
    SCHEDULER_LEADER_LOCK_TTL: int = 30
    SCHEDULER_JOB_SYNC_INTERVAL: int = 10  # Refresh task list from DB every N seconds
    SCHEDULER_MISFIRE_GRACE_SECONDS: int = 3600  # Re-run tasks missed within 1 hour
    SCHEDULER_DEDUP_TTL: int = 600  # Deduplication window: same task same minute only triggers once within 10 minutes

    # 部署模式：standalone（默认，单实例）/ ha_cluster（Redis 选举 + 队列高可用）
    SCHEDULER_DEPLOYMENT_MODE: Optional[str] = None  # 手动覆盖，可强制切换

    @property
    def deployment_mode(self) -> DeploymentMode:
        """解析最终部署模式。"""
        return _resolve_deployment_mode(self.SCHEDULER_DEPLOYMENT_MODE)

    @property
    def is_standalone(self) -> bool:
        return self.deployment_mode == "standalone"

    @property
    def is_ha_cluster(self) -> bool:
        return self.deployment_mode == "ha_cluster"

    # Backend API callback URL (optional: used when Worker does not connect directly to DB)
    BACKEND_API_BASE_URL: Optional[str] = None  # e.g.: http://127.0.0.1:8000
    BACKEND_API_TOKEN: Optional[str] = None

    # Health check
    HEALTH_HOST: str = "0.0.0.0"
    HEALTH_PORT: int = 9101

    # Timezone
    TIMEZONE: str = "Asia/Shanghai"

    @property
    def db_dsn(self) -> str:
        return (
            f"mysql+pymysql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}?charset={self.DB_CHARSET}"
        )

    @property
    def redis_dsn(self) -> str:
        if self.REDIS_URL:
            return self.REDIS_URL
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

    @property
    def script_task_table(self) -> str:
        return f"{self.DB_TABLE_PREFIX}script_task"

    @property
    def script_task_execution_table(self) -> str:
        return f"{self.DB_TABLE_PREFIX}script_task_execution"

    @property
    def schedule_table(self) -> str:
        return f"{self.DB_TABLE_PREFIX}schedule"

    @property
    def schedule_execution_table(self) -> str:
        return f"{self.DB_TABLE_PREFIX}schedule_execution"

    @property
    def workflow_table(self) -> str:
        return f"{self.DB_TABLE_PREFIX}workflow"

    @property
    def script_table(self) -> str:
        return f"{self.DB_TABLE_PREFIX}script"


@lru_cache
def get_settings() -> Settings:
    return Settings()