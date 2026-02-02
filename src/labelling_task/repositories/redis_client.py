from __future__ import annotations

import redis.asyncio as redis

from labelling_task.configs.settings import Settings


def get_redis(settings: Settings) -> redis.Redis:
    return redis.from_url(settings.redis_url, decode_responses=True)
