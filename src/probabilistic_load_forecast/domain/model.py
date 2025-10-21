"""
This module contains the core business entities.
"""

from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import List
from datetime import datetime
import pandas as pd


@dataclass
class LoadMeasurement:
    """
    Domain entity representing a single measurement of load data.
    """

    start_ts: str
    end_ts: str
    load_mw: float


@dataclass
class LoadTimeseries:
    """Domain entity representing a timeseries of load measurements."""

    data: pd.Series
    bidding_zone: str


class DataProvider(ABC):
    """
    Abstract interface (port) for retrieving measurements from a data source.

    Concrete implementations may fetch data from APIs, databases, files, etc.
    """

    @abstractmethod
    def get_data(
        self, start: datetime, end: datetime, **kwargs
    ) -> List[LoadMeasurement]:
        """Retrieve measurements within a given time range.
        Args:
            start (datetime): Start of the time window (inclusive).
            end (datetime): End of the time window (exclusive).
            **kwargs: Optional source-specific parameters.

        Returns:
            List[Measurement]: The measurements fetched from the data source.
        """
