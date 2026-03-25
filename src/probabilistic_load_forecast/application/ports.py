from abc import ABC, abstractmethod
from typing import List, Any
from datetime import datetime
from probabilistic_load_forecast.domain.model import (
    CountryCode,
    TimeInterval
)


class DataProvider(ABC):
    """
    Abstract interface (port) for retrieving measurements from a data source.

    Concrete implementations may fetch data from APIs, databases, files, etc.
    """

    @abstractmethod
    def get_data(
        self, interval: TimeInterval, **kwargs
    ) -> List[Any]:
        """Retrieve measurements within a given interval.
        Args:
            interval: Specifies the interval of the data to fetch
            **kwargs: Optional source-specific parameters.

        Returns:
            List[Any]: The measurements fetched from the data source.
        """


class CountryCodeNormalizer(ABC):
    """Abstract interface for normalizing country identifiers."""

    @abstractmethod
    def normalize(self, value: str) -> CountryCode:
        """Normalize a raw country identifier into an ISO alpha-2 code."""
