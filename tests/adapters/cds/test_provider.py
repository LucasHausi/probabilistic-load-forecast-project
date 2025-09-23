import pytest
from unittest.mock import Mock
from datetime import datetime
from probabilistic_load_forecast.adapters.cds.provider import CDSTimeFrame, CDSDataProvider
from probabilistic_load_forecast.adapters.cds.api_client import CDSConfig


# Tests for the CDSDataProvider
# -------------------------------------------------------------------------------------
@pytest.mark.parametrize(
    "variables,start,end,limit,expected",
    [
        (["t2m", "tp"], datetime(2025, 5, 1), datetime(2025, 5, 1), 12000, False), # under limit
        (["v" + str(i) for i in range(18)], datetime(2024, 5, 1), datetime(2025, 5, 31), 12000, True), # exceeds limit
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

@pytest.mark.parametrize(
    "start, end ,expected",
    [
       (datetime(2025, 1, 1), datetime(2025, 1, 4), [CDSTimeFrame(datetime(2025, 1, 1), datetime(2025, 1, 4))]),
       (
           datetime(2025, 1, 1),
           datetime(2025, 3, 30),
            [
               CDSTimeFrame(datetime(2025, 1, 1), datetime(2025, 1, 31)),
               CDSTimeFrame(datetime(2025, 2, 1), datetime(2025, 2, 28)),
               CDSTimeFrame(datetime(2025, 3, 1), datetime(2025, 3, 30))
            ]
        ),
         (
           datetime(2025, 12, 1),
           datetime(2026, 1, 30),
            [
               CDSTimeFrame(datetime(2025, 12, 1), datetime(2025, 12, 31)),
               CDSTimeFrame(datetime(2026, 1, 1), datetime(2026, 1, 30))
            ]
        )
    ]
)
def test_provider_calculates_correct_timeframes(start, end, expected):
    fetcher = Mock()
    mapper = Mock()
    provider = CDSDataProvider(fetcher, mapper)

    timeframes = provider._get_cds_timeframes(start, end)
    print(timeframes)
    assert len(timeframes) == len(expected)
    assert timeframes == expected
# Tests for the CDSTimeFrame
# -------------------------------------------------------------------------------------
def test_cds_timeframe_invalid_timeframe_raises():
    start = datetime(2025, 4, 5)
    end = datetime(2025, 4, 4)
    with pytest.raises(ValueError, match="End must be after start"):
        CDSTimeFrame(start, end)

def test_cds_timeframe_too_long_timeframe_raises():
    start = datetime(2025, 5, 1)
    end = datetime(2025, 8, 1)
    with pytest.raises(ValueError, match="The Timeframe is only allowed to be maximally a month"):
        CDSTimeFrame(start, end)

@pytest.mark.parametrize(
    "start,end,expected_days",
    [
        (datetime(2023, 5, 1), datetime(2023, 5, 1), ["01"]),
        (datetime(2023, 5, 1), datetime(2023, 5, 3), ["01", "02", "03"])
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
    assert result["year"] == "2023"
    assert result["month"] == "05"
    assert result["day"] == ["01"]
    assert result["time"][0] == "00:00"
    assert result["time"][-1] == "23:00"

@pytest.mark.parametrize(
    "start,end,expected_years",
    [
        (datetime(2023, 5, 1), datetime(2023, 5, 1), "2023")
    ],
)
def test_cds_timeframe_format_single_year(start, end, expected_years):
    tf = CDSTimeFrame(start, end)
    result = tf.to_dict()
    assert result["year"] == expected_years

@pytest.mark.parametrize(
    "start,end,expected_months",
    [
        (datetime(2023, 5, 1), datetime(2023, 5, 31), "05")
    ],
)
def test_cds_timeframe_format_single_month(start, end, expected_months):
    tf = CDSTimeFrame(start, end)
    result = tf.to_dict()
    assert result["month"] == expected_months
# -------------------------------------------------------------------------------------