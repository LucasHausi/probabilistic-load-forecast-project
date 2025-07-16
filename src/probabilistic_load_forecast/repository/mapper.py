from lxml import etree
from typing import List
from datetime import datetime, timedelta
from probabilistic_load_forecast.domain.entities import LoadData

class XML_Load_Mapper:
    @staticmethod
    def map(xml: str) -> List[LoadData]:
        ns = {'ns': 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'}
        tree = etree.fromstring(xml.encode())
        # Find Period
        period = tree.find('.//ns:Period', namespaces=ns)
        start_str = period.find('.//ns:start', namespaces=ns).text
        resolution = period.find('.//ns:resolution', namespaces=ns).text

        # Parse start time
        start_dt = datetime.fromisoformat(start_str)
        if resolution == 'PT15M':
            step = timedelta(minutes=15)
        else:
            raise ValueError('Only PT15M supported')

        result = []
        # Iterate over Point elements
        for point in period.findall('.//ns:Point', namespaces=ns):
            pos = int(point.find('ns:position', namespaces=ns).text)
            quantity = float(point.find('ns:quantity', namespaces=ns).text)
            ts = start_dt + step * (pos - 1)
            result.append(LoadData(timestamp=ts.isoformat(), value=quantity))
        return result