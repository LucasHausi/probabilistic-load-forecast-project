from itertools import chain
from datetime import datetime, timedelta
from probabilistic_load_forecast.domain.model import MeasurementProvider
from pprint import pprint
from dataclasses import dataclass
from probabilistic_load_forecast.adapters import utils

@dataclass(frozen=True)
class CDSTimeFrame:
    start: datetime
    end: datetime

    def __post_init__(self):
        if self.end < self.start:
            raise ValueError("End must be after start")
        
    def to_dict(self) -> dict:
        return {
            "year": [f"{y}" for y in range(self.start.year, self.end.year + 1)],
            "month": [f"{m:02d}" for m in range(self.start.month, self.end.month + 1)],
            "day": [f"{d:02d}" for d in range(self.start.day, self.end.day + 1)],
            "time": [f"{h:02d}:00" for h in range(24)],
        }



class CDSRepository(MeasurementProvider):
    def __init__(self, fetcher, mapper):
        self.fetcher = fetcher
        self.mapper = mapper

    def _exceeds_limit(self, timeframe: CDSTimeFrame) -> bool:
        cfg = self.fetcher.config
        days = (timeframe.end - timeframe.start).days + 1
        hours = 24
        n_vars = len(cfg.variable)
        field_count = n_vars * days * hours
        return field_count > cfg.field_limit
    
    def _get_max_chunksize(self) -> bool:
        cfg = self.fetcher.config
        n_vars = len(cfg.variable)
        days = cfg.field_limit // (n_vars * 24)
        return days

    def get_data(self, start, end, **kwargs):

        raw_data_locations = []

        # Possible UTC conversion here
        chunk_start = utils.to_utc(start)
        end = utils.to_utc(end)
        
        max_days = self._get_max_chunksize()

        while chunk_start < end:
            chunk_end = min(chunk_start+timedelta(days=max_days), end)

            datetime_cds_format = CDSTimeFrame(chunk_start, chunk_end).to_dict()

            # raw_data_location = self.fetcher.fetch(
            #     year=datetime_cds_format["year"],
            #     month=datetime_cds_format["month"],
            #     day=datetime_cds_format["day"],
            #     time=datetime_cds_format["time"],
            #     filename=kwargs.get("filename", f"era5_{datetime_cds_format["year"]}_{datetime_cds_format["month"]}.nc4.nc"),
            #     **kwargs
            # )

            # raw_data_locations.append(raw_data_location)
            pprint(datetime_cds_format)
            print(f"Querying: {chunk_start} to {chunk_end}")
            chunk_start = chunk_end + timedelta(hours=1)

        mapped_data = [self.mapper.map(data) for data in raw_data_locations if data is not None]
        return chain.from_iterable(mapped_data)
