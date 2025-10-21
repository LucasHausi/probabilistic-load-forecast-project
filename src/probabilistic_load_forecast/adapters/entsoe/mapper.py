"""Mapper for ENTSO-E XML load data to LoadMeasurement objects."""

from datetime import datetime, timedelta
from typing import List
from lxml import etree
from probabilistic_load_forecast.domain.model import LoadMeasurement


class XmlLoadMapper:
    """Mapper to convert ENTSO-E XML load data into LoadMeasurement objects."""

    @staticmethod
    def map(xml: str) -> List[LoadMeasurement]:
        """Map ENTSO-E XML load data to a list of LoadMeasurement objects."""

        ns = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
        tree = etree.fromstring(xml.encode())  # pylint: disable=c-extension-no-member
        # Find Period
        period = tree.find(".//ns:Period", namespaces=ns)
        start_str = period.find(".//ns:start", namespaces=ns).text
        resolution = period.find(".//ns:resolution", namespaces=ns).text

        # Parse start time
        start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
        if resolution == "PT15M":
            step = timedelta(minutes=15)
        else:
            raise ValueError("Only PT15M supported")

        result = []
        # Iterate over Point elements
        for point in period.findall(".//ns:Point", namespaces=ns):
            pos = int(point.find("ns:position", namespaces=ns).text)
            quantity = float(point.find("ns:quantity", namespaces=ns).text)
            start_ts = start_dt + step * (pos - 1)
            end_ts = start_ts + timedelta(minutes=15)
            load_measure = LoadMeasurement(
                start_ts=start_ts.strftime("%Y-%m-%d %H:%M"),
                end_ts=end_ts.strftime("%Y-%m-%d %H:%M"),
                load_mw=quantity,
            )
            result.append(load_measure)
        return result
