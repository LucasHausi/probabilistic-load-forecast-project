from itertools import chain
from typing import List
from datetime import datetime, timedelta, timezone
import calendar
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
            raise ValueError(
                "End must be after start. "
                f"Start: {self.start} "
                f"End: {self.end} "
            )
        if self.end.month != self.start.month:
            raise ValueError("The Timeframe is only allowed to be maximally a month")
        
    def to_dict(self) -> dict:
        return {
            "year": f"{self.start.year}",
            "month": f"{self.start.month:02d}",
            "day": [f"{d:02d}" for d in range(self.start.day, self.end.day + 1)],
            "time": [f"{h:02d}:00" for h in range(24)],
        }



class CDSDataProvider():
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
    
    def _get_cds_timeframes(self, start: datetime, end: datetime) -> List[CDSTimeFrame]:
        timeframes = []
        
        chunk_start = start

        while chunk_start < end:
            _, days_this_month = calendar.monthrange(chunk_start.year, chunk_start.month)
            remaining_days = days_this_month-chunk_start.day

            chunk_end = min(chunk_start+timedelta(days=remaining_days), end)

            timeframes.append(CDSTimeFrame(chunk_start, chunk_end))
            chunk_start = chunk_end + timedelta(days=1)

        return timeframes
    
    def get_data(self, start, end, **kwargs):

        raw_data_locations = []

        # Possible UTC conversion here
        start = utils.to_utc(start)
        end = utils.to_utc(end)

        timeframes = self._get_cds_timeframes(start, end)

        for timeframe in timeframes:

            datetime_cds_format = timeframe.to_dict()

            raw_data_location = self.fetcher.fetch(
                year=datetime_cds_format["year"],
                month=datetime_cds_format["month"],
                day=datetime_cds_format["day"],
                time=datetime_cds_format["time"],
                filename=kwargs.get("filename", f"era5_{datetime_cds_format["year"]}_{datetime_cds_format["month"]}.nc4.nc"),
                **kwargs
            )

            # filename=kwargs.get("filename", f"era5_{datetime_cds_format["year"]}_{datetime_cds_format["month"]}.nc4.nc")
            # raw_data_locations.append(raw_data_location)
            # print("Querying:")
            # print(filename)

        mapped_data = [self.mapper.map(data) for data in raw_data_locations if data is not None]
        return chain.from_iterable(mapped_data)
    
class InMemoryCDSDataProvider():
        def __init__(self, paths):
            self.paths = paths

        def retrieve(self):
            ...
