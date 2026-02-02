import redis.asyncio as redis

from labelling_task.configs.settings import get_settings
from labelling_task.configs.logging_config import get_logger

log = get_logger(__name__)


class RedisClient:
    """
    Simple Redis client wrapper, following the pattern used in user_management.
    """

    client: redis.Redis = None

    async def connect(self) -> None:
        settings = get_settings()
        try:
            log.info(f"Connecting to Redis at {settings.redis_url}")
            self.client = redis.from_url(settings.redis_url, decode_responses=True)
            await self.client.ping()
            log.info("Connected to Redis")
        except Exception as e:
            log.error(f"Error connecting to Redis: {e}")
            raise

    async def close(self) -> None:
        if self.client:
            await self.client.close()
            self.client = None


redis_client = RedisClient()
