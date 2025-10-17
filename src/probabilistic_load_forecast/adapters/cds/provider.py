from itertools import chain
from typing import List
from datetime import datetime, timedelta, timezone
import calendar
from probabilistic_load_forecast.domain.model import DataProvider
from probabilistic_load_forecast.adapters.cds import CDSTask
from pprint import pprint
from dataclasses import dataclass
from probabilistic_load_forecast.adapters import utils
import asyncio
import aiohttp
import aiofiles


@dataclass(frozen=True)
class CDSTimeFrame:
    start: datetime
    end: datetime

    def __post_init__(self):
        if self.end < self.start:
            raise ValueError(
                "End must be after start. " f"Start: {self.start} " f"End: {self.end} "
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


class CDSDataProvider:
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
            _, days_this_month = calendar.monthrange(
                chunk_start.year, chunk_start.month
            )
            remaining_days = days_this_month - chunk_start.day

            chunk_end = min(chunk_start + timedelta(days=remaining_days), end)

            timeframes.append(CDSTimeFrame(chunk_start, chunk_end))
            chunk_start = chunk_end + timedelta(days=1)

        return timeframes

    async def _poll_and_download(self, cds_tasks: List[CDSTask]) -> List[str]:
        timeout = aiohttp.ClientTimeout(total=600)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [
                self._poll_one(session, task, "./data/raw/cds") for task in cds_tasks
            ]
            results = await asyncio.gather(*tasks)
            return results

    async def _poll_one(
        self, session: aiohttp.ClientSession, task: CDSTask, target_path: str
    ):
        while True:
            async with session.get(url=task.url, headers=task.headers) as resp:
                response = await resp.json()
                state = response.get("status", "failed")
                if state == "successful":
                    links = response.get("links", [])
                    results_link = next(
                        (l["href"] for l in links if l["rel"] == "results"), None
                    )
                    if results_link is None:
                        raise RuntimeError(f"No results link in job status: {response}")

                    async with session.get(
                        results_link, headers=task.headers
                    ) as results_resp:
                        results_json = await results_resp.json()

                    # Step 2: extract the asset href
                    try:
                        asset_href = results_json["asset"]["value"]["href"]
                    except KeyError as exc:
                        raise RuntimeError(
                            f"No asset href in results JSON: {results_json}"
                        ) from exc

                    async with (
                        session.get(url=asset_href, headers=task.headers) as dl,
                        aiofiles.open(f"{target_path}/{task.identifier}.nc", "wb") as f,
                    ):
                        async for chunk in dl.content.iter_chunked(1024):
                            await f.write(chunk)

                    return target_path
                elif state == "failed":
                    raise RuntimeError(f"Task failed: {response}")
            await asyncio.sleep(30)

    def get_data(self, start, end, **kwargs):

        cds_tasks = []

        timeframes = self._get_cds_timeframes(start, end)

        for timeframe in timeframes:

            datetime_cds_format = timeframe.to_dict()

            raw_data_location = self.fetcher.fetch(
                year=datetime_cds_format["year"],
                month=datetime_cds_format["month"],
                day=datetime_cds_format["day"],
                time=datetime_cds_format["time"],
                **kwargs,
            )

            # filename=kwargs.get("filename", f"era5_{datetime_cds_format["year"]}_{datetime_cds_format["month"]}.nc4.nc")
            # print("Querying:")
            # print(datetime_cds_format)
            cds_tasks.append(raw_data_location)
        # print(cds_tasks)
        asyncio.run(self._poll_and_download(cds_tasks))

        mapped_data = [self.mapper.map(data) for data in cds_tasks if data is not None]
        return chain.from_iterable(mapped_data)


class InMemoryCDSDataProvider:
    def __init__(self, paths):
        self.paths = paths

    def retrieve(self): ...
