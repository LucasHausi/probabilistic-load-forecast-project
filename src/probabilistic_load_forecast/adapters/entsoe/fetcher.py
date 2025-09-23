"""
This module contains the logic for fetching data from the ENTSOE API
"""
from typing import List
from datetime import timedelta, datetime
from probabilistic_load_forecast.adapters.entsoe.api_client import EntsoeAPIClient
from probabilistic_load_forecast.domain.model import LoadMeasurement
from probabilistic_load_forecast.adapters import utils

MAX_TIMEINTERVAL = timedelta(days=365)
ENTSOE_FMT = "%Y%m%d%H%M"

def floor_to_minutes(dt: datetime, step:int) -> datetime:
    """
    Floor datetime to the nearest lower multiple of `step` minutes.
    """
    discard = dt.minute % step
    return dt.replace(minute=dt.minute - discard, second=0, microsecond=0)



class EntsoeFetcher():
    """
    This class wraps an EntsoeAPIClient and adds logic to handle
    API constraints, such as chunking requests.
    """
    def __init__(self, api_client: EntsoeAPIClient):
        self._api_client = api_client

    def fetch(self, start, end, **kwargs)-> List[LoadMeasurement]:
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

            period_start_utc = utils.to_utc(chunk_start)
            period_end_utc = utils.to_utc(chunk_end)

            query_params = {
                "documentType":"A65",
                "processType":"A16",
                "outBiddingZone_Domain":"10YAT-APG------L",
                "periodStart": period_start_utc.strftime(ENTSOE_FMT),
                "periodEnd": floor_to_minutes(period_end_utc, 15).strftime(ENTSOE_FMT),
                **kwargs
            }
            results.append(self._api_client.fetch_load_data(query_params))

            chunk_start = chunk_end

        return results
