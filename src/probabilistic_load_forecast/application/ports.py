from abc import ABC, abstractmethod
from typing import List
from datetime import datetime
from probabilistic_load_forecast.domain.model import CountryCode, LoadMeasurement

class DataProvider(ABC):
    """
    Abstract interface (port) for retrieving measurements from a data source.

    Concrete implementations may fetch data from APIs, databases, files, etc.
    """

    @abstractmethod
    def get_data(
        self, start:datetime, end:datetime, **kwargs
    ) -> List[LoadMeasurement]:
        """Retrieve measurements within a given time range.
        Args:
            start (datetime): Start of the time window (inclusive).
            end (datetime): End of the time window (exclusive).
            **kwargs: Optional source-specific parameters.

        Returns:
            List[LoadMeasurement]: The measurements fetched from the data source.
        """


class CountryCodeNormalizer(ABC):
    """Abstract interface for normalizing country identifiers."""

    @abstractmethod
    def normalize(self, value: str) -> CountryCode:
        """Normalize a raw country identifier into an ISO alpha-2 code."""
