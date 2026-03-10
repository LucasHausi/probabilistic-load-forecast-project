from datetime import datetime, timezone

from probabilistic_load_forecast.application.services.entsoe_services import (
    GetActualLoadData,
)
from probabilistic_load_forecast.domain.model import (
    BiddingZone,
    LoadMeasurement,
    LoadSeries,
    Resolution,
    TimeInterval,
)


class FakeLoadRepository:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def get(self, start, end, bidding_zone):
        self.calls.append((start, end, bidding_zone))
        return self.result


def test_get_actual_load_data_returns_series():
    start = datetime(2025, 7, 13, 0, 0, tzinfo=timezone.utc)
    end = datetime(2025, 7, 13, 0, 30, tzinfo=timezone.utc)
    bidding_zone = BiddingZone(
        eic_code="10YAT-APG------L",
        code="AT",
        display_name="Austria",
        country_code="AT",
    )

    expected_series = LoadSeries(
        bidding_zone=bidding_zone,
        resolution=Resolution.PT15M,
        observations=(
            LoadMeasurement(
                bidding_zone=bidding_zone,
                interval=TimeInterval(
                    start=datetime(2025, 7, 13, 0, 0, tzinfo=timezone.utc),
                    end=datetime(2025, 7, 13, 0, 15, tzinfo=timezone.utc),
                ),
                load_mw=4544.0,
            ),
            LoadMeasurement(
                bidding_zone=bidding_zone,
                interval=TimeInterval(
                    start=datetime(2025, 7, 13, 0, 15, tzinfo=timezone.utc),
                    end=datetime(2025, 7, 13, 0, 30, tzinfo=timezone.utc),
                ),
                load_mw=4521.0,
            ),
        ),
    )

    repo = FakeLoadRepository(expected_series)
    use_case = GetActualLoadData(repo)

    result = use_case(start, end, bidding_zone)

    assert result == expected_series
    assert repo.calls == [(start, end, bidding_zone)]


def test_get_actual_load_data_returns_empty_series():
    start = datetime(2025, 7, 13, 0, 0, tzinfo=timezone.utc)
    end = datetime(2025, 7, 13, 0, 30, tzinfo=timezone.utc)
    bidding_zone = bidding_zone = BiddingZone(
        eic_code="10YAT-APG------L",
        code="AT",
        display_name="Austria",
        country_code="AT",
    )

    empty_series = LoadSeries(
        bidding_zone=bidding_zone,
        resolution=Resolution.PT15M,
        observations=(),
    )

    repo = FakeLoadRepository(empty_series)
    use_case = GetActualLoadData(repo)

    result = use_case(start, end, bidding_zone)

    assert result == empty_series
    assert result.observations == ()
