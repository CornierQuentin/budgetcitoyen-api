"""Client Redis asynchrone partage par l'application."""

import redis.asyncio as redis

from api.core.config import get_settings

settings = get_settings()

redis_client = redis.Redis.from_url(settings.redis_url, decode_responses=True)
