from probabilistic_load_forecast.infrastructure.api_client import EntsoeAPIClient
from .mapper import XML_Load_Mapper

class EntsoeFetcher():
    def __init__(self, api_client: EntsoeAPIClient):
        self._api_client = api_client
        self._mapper = XML_Load_Mapper()
    
    def fetch(self, params):
        return self._api_client.fetch_load_data(params)