from probabilistic_load_forecast.infrastructure.api_client import EntsoeAPIClient
from .mapper import XML_Load_Mapper

class EntsoeFetcher():
    def __init__(self, api_client: EntsoeAPIClient):
        self._api_client = api_client
        self._mapper = XML_Load_Mapper()
    
    def get_load_data(self, params):
        load_xml = self._api_client.fetch_load_data(params)
        self._mapper.parse_xml_load_data(load_xml)