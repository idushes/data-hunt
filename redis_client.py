import logging

from redis.asyncio import Redis

from config import REDIS_URL

logger = logging.getLogger(__name__)

_redis_client: Redis | None = None


def get_redis_client() -> Redis | None:
    global _redis_client
    if not REDIS_URL:
        return None
    if _redis_client is None:
        _redis_client = Redis.from_url(
            REDIS_URL,
            decode_responses=False,
            socket_connect_timeout=2,
            socket_timeout=3,
            health_check_interval=30,
        )
    return _redis_client


async def redis_ping() -> bool:
    client = get_redis_client()
    if client is None:
        return False
    return bool(await client.ping())


async def close_redis_client() -> None:
    global _redis_client
    if _redis_client is None:
        return
    await _redis_client.aclose()
    _redis_client = None
