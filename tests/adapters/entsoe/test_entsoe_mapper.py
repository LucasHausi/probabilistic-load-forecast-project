from datetime import datetime, timezone
from probabilistic_load_forecast.adapters.entsoe.mapper import XmlLoadMapper
from probabilistic_load_forecast.domain.model import LoadMeasurement, BiddingZone, TimeInterval, CountryCode


def test_parse_xml_load_data():
    with open("tests/fixtures/sample_load.xml", encoding="utf-8") as f:
        xml = f.read()
    measurements = XmlLoadMapper.map(xml)
    assert isinstance(measurements, list)
    assert measurements[0] == LoadMeasurement(
        bidding_zone=BiddingZone(
            eic_code="10YAT-APG------L",
            display_name="Austria",
            country_code= CountryCode("AT"),
        ),
        interval=TimeInterval(
            start=datetime(2025, 7, 13, 0, 0, tzinfo=timezone.utc),
            end=datetime(2025, 7, 13, 0, 15, tzinfo=timezone.utc),
        ),
        load_mw=4544.0,
    )
    assert measurements[95] == LoadMeasurement(
        bidding_zone=BiddingZone(
            eic_code="10YAT-APG------L",
            display_name="Austria",
            country_code= CountryCode("AT"),
        ),
        interval=TimeInterval(
            start=datetime(2025, 7, 13, 23, 45, tzinfo=timezone.utc),
            end=datetime(2025, 7, 14, 0, 0, tzinfo=timezone.utc),
        ),
        load_mw=4836.0,
    )
