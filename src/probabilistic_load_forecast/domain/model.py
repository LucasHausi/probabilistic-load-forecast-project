"""
This module contains the core business entities.
"""
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import List, Tuple, Dict
from datetime import datetime

@dataclass
class Measurement:
    """
    Domain entity representing a single measurement of load data.
    
    Attributes:
        timestamp (str): The time at which the measurement was taken.
        value (float): The measured load value.
    """
    timestamp: str
    value: float

@dataclass
class SpatioTemporalMeasurement:
    timestamp: datetime
    location: Tuple[float, float]
    variables: Dict[str, float]

class MeasurementProvider(ABC):
    """
    Abstract interface (port) for retrieving measurements from a data source.

    Concrete implementations may fetch data from APIs, databases, files, etc.
    """
    @abstractmethod
    def get_data(self, start: datetime, end: datetime, **kwargs) -> List[Measurement]:
        """Retrieve measurements within a given time range.
        Args:
            start (datetime): Start of the time window (inclusive).
            end (datetime): End of the time window (exclusive).
            **kwargs: Optional source-specific parameters.

        Returns:
            List[Measurement]: The measurements fetched from the data source.
        """