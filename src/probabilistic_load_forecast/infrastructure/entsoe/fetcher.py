"""
This module contains the logic for fetching data from the ENTSOE API
"""
from typing import List
from datetime import timedelta
from probabilistic_load_forecast.infrastructure.entsoe.api_client import EntsoeAPIClient
from probabilistic_load_forecast.domain.entities import Measurement
MAX_TIMEINTERVAL = timedelta(days=365)

class EntsoeFetcher():
    """
    This class wraps an EntsoeAPIClient and adds logic to handle
    API constraints, such as chunking requests.
    """
    def __init__(self, api_client: EntsoeAPIClient):
        self._api_client = api_client

    def fetch(self, start, end, **kwargs)-> List[Measurement]:
        """Fetches the data from the ENTSOE API given the timeframe"
        "and handles the chunking logic if the timeframe is larger then the API limit.

        Args:
            start (datetime): Start of the time window (inclusive).
            end (datetime): End of the time window (exclusive).
            **kwargs: Optional source-specific parameters.

        Returns:
            List[Measurement]: The measurements fetched from the data source.
        """
        results = []
        chunk_start = start

        while chunk_start < end:
            chunk_end = min(end, chunk_start+MAX_TIMEINTERVAL)

            query_params = {
                "documentType":"A65",
                "processType":"A16",
                "outBiddingZone_Domain":"10YAT-APG------L",
                "periodStart": chunk_start.strftime("%Y%m%d%H%M"),
                "periodEnd": chunk_end.strftime("%Y%m%d%H%M"),
                **kwargs
            }
            results.append(self._api_client.fetch_load_data(query_params))

            chunk_start = chunk_end

        return results
