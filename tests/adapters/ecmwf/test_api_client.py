from pathlib import Path
from unittest.mock import ANY, Mock, call
from datetime import datetime, timezone, date
from probabilistic_load_forecast.adapters.ecmwf.api_client import ECMWFAPIClient

from probabilistic_load_forecast.domain.model import TimeInterval, WeatherVariable


def test_fetching_returns_file_locations():
    mock_client = Mock()

    api_client = ECMWFAPIClient(target_dir=Path("./"), client=mock_client)

    interval = TimeInterval(
        start=datetime(2026, 3, 10, 0, 0, 0, tzinfo=timezone.utc),
        end=datetime(2026, 3, 15, 0, 0, 0, tzinfo=timezone.utc),
    )

    excpected = [
        "2026-03-09.grib2",
        "2026-03-10.grib2",
        "2026-03-11.grib2",
        "2026-03-12.grib2",
        "2026-03-13.grib2"
    ]

    result = api_client.fetch(interval=interval, weather_variables=[WeatherVariable.T2M])

    assert result == excpected
    mock_client.retrieve.assert_has_calls(
        [
            call(
                date=date(2026, 3, 9),
                time=12,
                type="fc",
                step=[12, 15, 18, 21, 24, 27, 30, 33, 36],
                param=["2t"],
                target=ANY,
            ),
            call(
                date=date(2026, 3, 10),
                time=12,
                type="fc",
                step=[12, 15, 18, 21, 24, 27, 30, 33, 36],
                param=["2t"],
                target=ANY,
            ),
            call(
                date=date(2026, 3, 11),
                time=12,
                type="fc",
                step=[12, 15, 18, 21, 24, 27, 30, 33, 36],
                param=["2t"],
                target=ANY,
            ),
            call(
                date=date(2026, 3, 12),
                time=12,
                type="fc",
                step=[12, 15, 18, 21, 24, 27, 30, 33, 36],
                param=["2t"],
                target=ANY,
            ),
            call(
                date=date(2026, 3, 13),
                time=12,
                type="fc",
                step=[12, 15, 18, 21, 24, 27, 30, 33, 36],
                param=["2t"],
                target=ANY,
            ),
        ]
    )
    assert mock_client.retrieve.call_count == 5

def test_forecast_issue_dates_for_one_day_interval():
    mock_client = Mock()

    api_client = ECMWFAPIClient(target_dir=Path("./"), client=mock_client)

    interval = TimeInterval(
        start=datetime(2026, 3, 10, 0, 0, 0, tzinfo=timezone.utc),
        end=datetime(2026, 3, 11, 0, 0, 0, tzinfo=timezone.utc),
    )

    expected = [
        date(2026, 3, 9)
    ]

    result = api_client.forecast_issue_dates_for(interval)

    assert result == expected

def test_forecast_issue_dates_for_multiy_day_interval():
    mock_client = Mock()

    api_client = ECMWFAPIClient(target_dir=Path("./"), client=mock_client)

    interval = TimeInterval(
        start=datetime(2026, 3, 10, 0, 0, 0, tzinfo=timezone.utc),
        end=datetime(2026, 3, 12, 10, 0, 0, tzinfo=timezone.utc),
    )

    expected = [
        date(2026, 3, 9),
        date(2026, 3, 10),
        date(2026, 3, 11),
    ]

    result = api_client.forecast_issue_dates_for(interval)

    assert result == expected
