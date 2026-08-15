"""Decorateur de mise en cache Redis pour des fonctions asynchrones."""

import json
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, TypeVar

from api.cache.redis_client import redis_client

T = TypeVar("T")


def cached(ttl: int) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Met en cache le resultat JSON-serialisable d'une coroutine pendant `ttl` secondes."""

    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            cache_key = f"{func.__module__}.{func.__qualname__}:{args}:{kwargs}"

            cached_value = await redis_client.get(cache_key)
            if cached_value is not None:
                result: T = json.loads(cached_value)
                return result

            result = await func(*args, **kwargs)
            await redis_client.set(cache_key, json.dumps(result), ex=ttl)
            return result

        return wrapper

    return decorator
