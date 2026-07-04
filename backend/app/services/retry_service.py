"""
Pure functions for computing retry backoff delays. Kept separate from the
job lifecycle logic so the strategies are trivial to unit test in isolation.
"""
from app.models import RetryStrategy


def compute_delay_seconds(
    strategy: RetryStrategy,
    attempt_number: int,
    base_delay_seconds: int,
    max_delay_seconds: int,
) -> int:
    """
    attempt_number is 1-indexed: the delay returned is how long to wait
    before *this* retry attempt runs, given how many attempts already failed.

    fixed:       base_delay
    linear:      base_delay * attempt_number
    exponential: base_delay * 2^(attempt_number - 1)
    """
    if attempt_number < 1:
        attempt_number = 1

    if strategy == RetryStrategy.FIXED:
        delay = base_delay_seconds
    elif strategy == RetryStrategy.LINEAR:
        delay = base_delay_seconds * attempt_number
    elif strategy == RetryStrategy.EXPONENTIAL:
        delay = base_delay_seconds * (2 ** (attempt_number - 1))
    else:
        delay = base_delay_seconds

    return min(delay, max_delay_seconds)
