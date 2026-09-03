"""
Taurus Scheduler - Redis distributed lock (simplified implementation based on Redlock)
Supports active-standby mode Leader Election, ensures only one Leader schedules
when multiple instances are deployed
"""
from __future__ import annotations

import time
import uuid
from typing import Optional

import redis

from .config import get_settings

import structlog

logger = structlog.get_logger()


class RedisDistributedLock:
    def __init__(self, redis_client: redis.Redis, name: str, ttl: int = 30):
        self.redis = redis_client
        self.name = name
        self.ttl = ttl
        self._token: Optional[str] = None

    def acquire(self, blocking: bool = True, retry_interval: float = 1.0, timeout: float = 0) -> bool:
        """
        Acquire the lock
        :param blocking: Whether to block and wait
        :param retry_interval: Retry interval in seconds
        :param timeout: Timeout duration, 0 means wait forever
        """
        start = time.time()
        self._token = uuid.uuid4().hex

        while True:
            if self.redis.set(self.name, self._token, nx=True, ex=self.ttl):
                logger.debug("lock.acquired", name=self.name, token=self._token)
                return True

            if not blocking:
                return False

            if timeout > 0 and (time.time() - start) > timeout:
                logger.warning("lock.timeout", name=self.name)
                return False

            time.sleep(retry_interval)

    def release(self) -> bool:
        """Release the lock (only releases if held by self)"""
        if not self._token:
            return False

        lua_script = """
        if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('del', KEYS[1])
        else
            return 0
        end
        """
        try:
            result = self.redis.eval(lua_script, 1, self.name, self._token)
            if result:
                logger.debug("lock.released", name=self.name, token=self._token)
            return bool(result)
        except Exception as e:
            logger.error("lock.release_failed", name=self.name, error=str(e))
            return False
        finally:
            self._token = None

    def extend(self, extra_ttl: Optional[int] = None) -> bool:
        """Renew (extend) the lock TTL"""
        if not self._token:
            return False
        ttl = extra_ttl or self.ttl
        lua = """
        if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('expire', KEYS[1], ARGV[2])
        else
            return 0
        end
        """
        result = self.redis.eval(lua, 1, self.name, self._token, ttl)
        return bool(result)

    def is_owner(self) -> bool:
        if not self._token:
            return False
        current = self.redis.get(self.name)
        return current and current.decode() == self._token


class LeaderElector:
    """Active-standby elector: ensures only one Scheduler instance is actively scheduling at any time

    M1.9 — standalone 模式（社区版 CE 单实例非 HA 降级）：
        不依赖 Redis，始终立即认为自己是 Leader。
        代价：部署多台 CE 版 scheduler 时会产生重复触发；CE 场景明确告知用户只允许跑 1 个实例。
    """

    def __init__(self, redis_client: redis.Redis | None):
        settings = get_settings()
        self._standalone = settings.is_standalone
        self.redis = redis_client
        self.instance_id = settings.SCHEDULER_INSTANCE_ID
        if not self._standalone:
            assert redis_client is not None, "ha_cluster 模式必须提供 Redis 客户端"
            self.lock = RedisDistributedLock(
                redis_client,
                name=settings.SCHEDULER_LEADER_LOCK_KEY,
                ttl=settings.SCHEDULER_LEADER_LOCK_TTL,
            )
        self._is_leader = False

    @property
    def is_leader(self) -> bool:
        if self._standalone:
            # CE standalone 永远是 Leader（不做 Redis 所有权检查）
            return self._is_leader
        return self._is_leader and self.lock.is_owner()

    def try_become_leader(self) -> bool:
        """Try to become Leader (non-blocking)"""
        if self._standalone:
            # standalone 模式：直接认定成功（真正的"降级"）
            if not self._is_leader:
                logger.info("leader.elected", mode="standalone", instance_id=self.instance_id)
            self._is_leader = True
            return True
        if self._is_leader and self.lock._token:
            if self.lock.is_owner():
                return True
        acquired = self.lock.acquire(blocking=False)
        if acquired:
            if not self._is_leader:
                logger.info("leader.elected", mode="ha_cluster", instance_id=self.instance_id)
            self._is_leader = True
        else:
            if self._is_leader:
                logger.warning("leader.lost", mode="ha_cluster", instance_id=self.instance_id)
            self._is_leader = False
        return self._is_leader

    def keepalive(self) -> bool:
        """Leader heartbeat renewal"""
        if self._standalone:
            # standalone 无需保活
            return self._is_leader
        if self._is_leader:
            ok = self.lock.extend()
            if not ok:
                logger.warning("leader.extend_failed", instance_id=self.instance_id)
                self._is_leader = False
        return self._is_leader

    def step_down(self) -> None:
        """Voluntarily step down as Leader"""
        if self._is_leader:
            if not self._standalone:
                self.lock.release()
            logger.info("leader.stepped_down",
                        mode="standalone" if self._standalone else "ha_cluster",
                        instance_id=self.instance_id)
            self._is_leader = False