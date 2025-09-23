"""
This module contains the basic logic to fetch data from the public CDS(Climate Data Store) API Endpoints.
"""
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List
import requests

TIMEOUT = 30
logger = logging.getLogger(__name__)

class CDSDataUnavailable(Exception):
    """Generic CDS data error."""

class CDSLicenseError(CDSDataUnavailable):
    """Raised when required CDS licences are not accepted."""

@dataclass
class CDSTask:
    url: str
    headers: dict
    session: object

@dataclass
class CDSConfig():
    dataset: str
    variable: List[str]
    area: List[float]
    field_limit: int

class CDSAPIClient():
    def __init__(self, client, download_dir: str = "./data/raw", config:CDSConfig = None):
        self._timeout = TIMEOUT
        self.client = client
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.config = config

    def fetch(
            self,
            year: str,
            month: str,
            day: List[str],
            time: List[str],
            # filename: str,
            download_format: str = "unarchived",
            data_format: str = "netcdf"
        ) -> CDSTask:

        request = {
            "dataset" : self.config.dataset,
            "variable": self.config.variable,
            "year": year,
            "month": month,
            "day": day,
            "time": time,
            "download_format": download_format,
            "data_format": data_format,
            "area": self.config.area,
        }

        # target = self.download_dir / filename
        try:
            remote = self.client.retrieve(self.config.dataset, request)
            return CDSTask(
                url=remote.url,
                headers=remote.headers,
                session=remote.session,
            )
            task = self.client.retrieve(self.config.dataset, request)#, target)
            return task
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 403:
                logger.error("CDS license not accepted for dataset %s", self.config.dataset)
                raise CDSLicenseError(
                    f"Licenses not accepted for dataset {self.config.dataset}"
                ) from e
            logger.error("HTTP error fetching data: %s", e)
            raise CDSDataUnavailable("Unexpected CDS API error") from e
        except Exception as e:
            logger.error("Unexpected error fetching data: %s", e)
            raise CDSDataUnavailable("Unexpected CDS API error") from e
