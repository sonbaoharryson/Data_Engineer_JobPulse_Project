import os

# Rate limiting is disabled by default. Set USE_RATE_LIMITING=true in the environment to enable.
_USE_RATE_LIMITING = os.getenv("USE_RATE_LIMITING", "false").lower() in ("1", "true", "yes")

if _USE_RATE_LIMITING:
    from slowapi import Limiter
    from slowapi.util import get_remote_address
    limiter = Limiter(key_func=get_remote_address)
else:
    class _NoOpLimiter:
        def limit(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator

    limiter = _NoOpLimiter()
