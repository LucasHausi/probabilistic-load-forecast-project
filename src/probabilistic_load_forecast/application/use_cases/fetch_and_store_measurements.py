"""
Application use case for fetching load measurements and persisting them.   
"""
class FetchAndStoreMeasurements:
    """
    Use case that fetches load measurements for a given time range
    and stores them in a repository.

    This class depends on:
      - fetcher: a MeasurementProvider that supplies measurement data
      - repo: a Repository responsible for persisting the data
    """
    def __init__(self, fetcher, repo):
        self.fetcher = fetcher
        self.repo = repo

    def __call__(self, start, end):
        measurements = list(self.fetcher.get_data(start, end))
        self.repo.save(measurements)
        return measurements
