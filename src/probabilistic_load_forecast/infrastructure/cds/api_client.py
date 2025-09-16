"""
This module contains the basic logic to fetch data from the public CDS(Climate Data Store) API Endpoints.
"""
import logging
import requests
from pathlib import Path
from typing import List

TIMEOUT = 30
logger = logging.getLogger(__name__)

class CDSDataUnavailable(Exception):
    """Generic CDS data error."""

class CDSLicenseError(CDSDataUnavailable):
    """Raised when required CDS licences are not accepted."""

class CDSAPIClient():
    def __init__(self, client, download_dir: str = "./data/raw"):
        self._timeout = TIMEOUT
        self.client = client
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
    def fetch_data(self, dataset, product_type: List, variable: List,
                    year: str, month: str, day: List, time: List,
                    area: List, filename: str,
                    download_format: str = "unarchived",
                    data_format: str = "grib"
                    ):
            # Define the request dict
            request = {
                "product_type": product_type,
                "variable": variable,
                "year": year,
                "month": month,
                "day": day,
                "time": time,
                "download_format": download_format,
                "data_format": data_format,
                "area": area
            }
            target = self.download_dir / filename
            try:
                self.client.retrieve(dataset, request, target)
                return target
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 403:
                    logger.error("CDS license not accepted for dataset %s", dataset)
                    raise CDSLicenseError(
                        f"Licenses not accepted for dataset {dataset}"
                    ) from e
                logger.error("HTTP error fetching data: %s", e)
                raise CDSDataUnavailable("Unexpected CDS API error") from e
            except Exception as e:
                logger.error("Unexpected error fetching data: %s", e)
                raise CDSDataUnavailable("Unexpected CDS API error") from e
