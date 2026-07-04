import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.models import RetryStrategy
from app.services.retry_service import compute_delay_seconds


def test_fixed_strategy_is_constant():
    assert compute_delay_seconds(RetryStrategy.FIXED, 1, base_delay_seconds=10, max_delay_seconds=999) == 10
    assert compute_delay_seconds(RetryStrategy.FIXED, 5, base_delay_seconds=10, max_delay_seconds=999) == 10


def test_linear_strategy_scales_with_attempt():
    assert compute_delay_seconds(RetryStrategy.LINEAR, 1, base_delay_seconds=5, max_delay_seconds=999) == 5
    assert compute_delay_seconds(RetryStrategy.LINEAR, 3, base_delay_seconds=5, max_delay_seconds=999) == 15


def test_exponential_strategy_doubles_each_attempt():
    assert compute_delay_seconds(RetryStrategy.EXPONENTIAL, 1, base_delay_seconds=2, max_delay_seconds=999) == 2
    assert compute_delay_seconds(RetryStrategy.EXPONENTIAL, 2, base_delay_seconds=2, max_delay_seconds=999) == 4
    assert compute_delay_seconds(RetryStrategy.EXPONENTIAL, 4, base_delay_seconds=2, max_delay_seconds=999) == 16


def test_delay_is_capped_at_max_delay():
    assert compute_delay_seconds(RetryStrategy.EXPONENTIAL, 10, base_delay_seconds=5, max_delay_seconds=60) == 60


def test_attempt_number_below_one_is_treated_as_one():
    assert compute_delay_seconds(RetryStrategy.LINEAR, 0, base_delay_seconds=5, max_delay_seconds=999) == 5
