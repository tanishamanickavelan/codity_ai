"""
Rate limiting, keyed by client IP address, using slowapi (a Flask-Limiter
style wrapper around FastAPI/Starlette).

Applied conservatively to the endpoints most worth protecting: auth
(brute-force / credential stuffing) and job creation (a runaway script or
misbehaving client shouldn't be able to flood a queue). Other endpoints are
left unlimited by default; add `@limiter.limit(...)` to any router function
that needs its own budget - just remember slowapi requires the decorated
function to accept a `request: Request` parameter (it inspects the caller's
IP from it).
"""
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
