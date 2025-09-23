import pytest
from unittest.mock import Mock
from datetime import datetime
from probabilistic_load_forecast.adapters.cds.provider import CDSTimeFrame, CDSDataProvider
from probabilistic_load_forecast.adapters.cds.api_client import CDSConfig

@pytest.mark.parametrize(
    "variables,start,end,limit,expected",
    [
        (["t2m", "tp"], datetime(2025, 5, 1), datetime(2025, 5, 1), 12000, False), # under limit
        (["t2m", "tp"], datetime(2024, 5, 1), datetime(2025, 12, 2), 12000, True), # exceeds limit
    ]
)
def test_config_exeeds_limit(variables, start, end, limit, expected):
    cfg = CDSConfig(
    dataset="era5",
    variable=variables,
    area=[49.03, 9.5, 46.35, 17.17], # Bounding box for austria
    field_limit=limit
)
    mock_fetcher = Mock()
    mock_fetcher.config = cfg

    provider = CDSDataProvider(fetcher=mock_fetcher, mapper=Mock())

    tf = CDSTimeFrame(start, end)
    result = provider._exceeds_limit(tf)
    print("result", result)
    assert result is expected


# Tests for the CDSTimeFrame
# -------------------------------------------------------------------------------------

# Tests for the CDSTimeFrame
# -------------------------------------------------------------------------------------
def test_cds_timeframe_invalid_timeframe_raises():
    start = datetime(2025, 5, 1)
    end = datetime(2025, 4, 1)
    with pytest.raises(ValueError, match="End must be after start"):
        CDSTimeFrame(start, end)

@pytest.mark.parametrize(
    "start,end,expected_days",
    [
        (datetime(2023, 5, 1), datetime(2023, 5, 1), ["01"]),
        (datetime(2023, 5, 1), datetime(2023, 5, 3), ["01", "02", "03"]),
    ],
)
def test_cds_timeframe_format_multiple_days(start, end, expected_days):
    tf = CDSTimeFrame(start, end)
    result = tf.to_dict()
    assert result["day"] == expected_days

def test_cds_timeframe_format_single_day():
    start = datetime(2023, 5, 1)
    end = datetime(2023, 5, 1)
    tf = CDSTimeFrame(start, end)

    result = tf.to_dict()
    assert result["year"] == ["2023"]
    assert result["month"] == ["05"]
    assert result["day"] == ["01"]
    assert result["time"][0] == "00:00"
    assert result["time"][-1] == "23:00"

@pytest.mark.parametrize(
    "start,end,expected_years",
    [
        (datetime(2023, 5, 1), datetime(2023, 5, 1), ["2023"]),
        (datetime(2023, 5, 1), datetime(2025, 5, 1), ["2023", "2024", "2025"]),
    ],
)
def test_cds_timeframe_format_multiple_years(start, end, expected_years):
    tf = CDSTimeFrame(start, end)
    result = tf.to_dict()
    assert result["year"] == expected_years

@pytest.mark.parametrize(
    "start,end,expected_months",
    [
        (datetime(2023, 5, 1), datetime(2023, 5, 1), ["05"]),
        (datetime(2023, 5, 1), datetime(2025, 7, 1), ["05", "06", "07"]),
    ],
)
def test_cds_timeframe_format_multiple_months(start, end, expected_months):
    tf = CDSTimeFrame(start, end)
    result = tf.to_dict()
    assert result["month"] == expected_months
# -------------------------------------------------------------------------------------