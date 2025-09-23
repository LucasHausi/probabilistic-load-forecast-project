from probabilistic_load_forecast.adapters.entsoe.mapper import XmlLoadMapper
from probabilistic_load_forecast.domain.model import Measurement


def test_parse_xml_load_data():
    with open("tests/fixtures/sample_load.xml", encoding="utf-8") as f:
        xml = f.read()
    measurements = XmlLoadMapper.map(xml)
    assert isinstance(measurements, list)
    assert measurements[0] == Measurement(timestamp="2025-07-13T00:00:00+00:00", value=4544)
    assert measurements[1] == Measurement(timestamp="2025-07-13T00:15:00+00:00", value=4529)
