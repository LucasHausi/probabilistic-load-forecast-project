from probabilistic_load_forecast.adapters.entsoe.mapper import XmlLoadMapper
from probabilistic_load_forecast.domain.model import LoadMeasurement


def test_parse_xml_load_data():
    with open("tests/fixtures/sample_load.xml", encoding="utf-8") as f:
        xml = f.read()
    measurements = XmlLoadMapper.map(xml)
    assert isinstance(measurements, list)
    assert measurements[0] == LoadMeasurement(
        start_ts="2025-07-13 00:00", end_ts="2025-07-13 00:15", load_mw=4544
    )
    assert measurements[95] == LoadMeasurement(
        start_ts="2025-07-13 23:45", end_ts="2025-07-14 00:00", load_mw=4836
    )
