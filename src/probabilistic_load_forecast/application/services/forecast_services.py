from probabilistic_load_forecast.adapters.db import(
    ForecastMetadataRepository
)
class GetLatestCommonTimestamp():
    def __init__(self, repo: ForecastMetadataRepository):
        self.repo = repo

    def __call__(self):
        return self.repo.get_latest_common_timestamp()