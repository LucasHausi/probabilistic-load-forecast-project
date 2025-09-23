from datetime import datetime, timezone
import pytest
from probabilistic_load_forecast.adapters.entsoe.fetcher import floor_to_minutes
from probabilistic_load_forecast.adapters.utils import to_utc

def test_floor_to_minutes_rounds_down():
    dt = datetime(2023, 1, 1, 10, 7)
    floored = floor_to_minutes(dt, 15)
    assert floored == datetime(2023, 1, 1, 10, 0)

def test_floor_to_minutes_exact_boundary():
    dt = datetime(2023, 1, 1, 10, 30)
    assert floor_to_minutes(dt, 15) == dt

def test_to_utc_converts_correctly():
    local = datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc)
    utc = to_utc(local)
    assert utc.tzinfo == timezone.utc
    assert utc.hour == 12

def test_to_utc_raises_on_naive():
    naive = datetime(2023, 1, 1, 12, 0)
    with pytest.raises(ValueError):
        to_utc(naive)
