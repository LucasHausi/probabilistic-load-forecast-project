"""
Application use case for fetching load measurements and persisting them.
"""

from probabilistic_load_forecast.domain.model import DataProvider


class FetchAndStoreMeasurements:
    """
    Use case that fetches load measurements for a given time range
    and stores them in a repository.

    This class depends on:
      - dataprovider: a MeasurementProvider that supplies measurement data
      - repo: a Repository responsible for persisting the data
    """

    def __init__(self, provider: DataProvider, repo):
        self.dataprovider = provider
        self.repo = repo

    def __call__(self, start, end):
        measurements = list(self.dataprovider.get_data(start, end))
        self.repo.add(measurements)


class GetActualLoadData:
    """Use case that retrieves actual load data from a repository."""

    def __init__(self, repo):
        self.repo = repo

    def __call__(self, start, end):
        return self.repo.get(start, end)
