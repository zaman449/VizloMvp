import asyncio, random, time, functools
from typing import Tuple, Type, Callable, Any

def retry_backoff(
    errors: Tuple[Type[Exception], ...] = (Exception,),
    max_retries: int = 5,
    first_wait: float = 1.0,
    jitter: bool = True,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator that retries a function with exponential back	off.
    Works for both sync and async callables.
    """
    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        is_coro = asyncio.iscoroutinefunction(fn)

        async def _async(*args, **kwargs):
            delay = first_wait
            for attempt in range(max_retries):
                try:
                    return await fn(*args, **kwargs)
                except errors:
                    if attempt == max_retries - 1:
                        raise
                    wait = delay + random.uniform(0, delay * 0.1) if jitter else delay
                    await asyncio.sleep(wait)
                    delay *= 2

        def _sync(*args, **kwargs):
            delay = first_wait
            for attempt in range(max_retries):
                try:
                    return fn(*args, **kwargs)
                except errors:
                    if attempt == max_retries - 1:
                        raise
                    wait = delay + random.uniform(0, delay * 0.1) if jitter else delay
                    time.sleep(wait)
                    delay *= 2

        return functools.wraps(fn)(_async if is_coro else _sync)

    return decorator

