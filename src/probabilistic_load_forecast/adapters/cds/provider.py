"""CDS Data Provider for fetching and downloading datasets."""

import random
import logging
import time
from typing import List
from datetime import datetime, timedelta
import calendar
from dataclasses import dataclass
import asyncio
import aiohttp
import aiofiles

from probabilistic_load_forecast.adapters.cds import CDSTask

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass(frozen=True)
class CDSTimeFrame:
    """A timeframe for CDS data requests."""

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
        """Convert the timeframe to a CDS-compatible dictionary."""
        return {
            "year": f"{self.start.year}",
            "month": f"{self.start.month:02d}",
            "day": [f"{d:02d}" for d in range(self.start.day, self.end.day + 1)],
            "time": [f"{h:02d}:00" for h in range(24)],
        }


class CDSDataProvider:
    """A data provider to fetch and download CDS datasets."""

    def __init__(self, fetcher):
        self.fetcher = fetcher

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

        while chunk_start <= end:
            _, days_this_month = calendar.monthrange(
                chunk_start.year, chunk_start.month
            )
            remaining_days = days_this_month - chunk_start.day

            chunk_end = min(chunk_start + timedelta(days=remaining_days), end)

            timeframes.append(CDSTimeFrame(chunk_start, chunk_end))
            chunk_start = chunk_end + timedelta(days=1)

        return timeframes

    async def _poll_and_download(self, cds_tasks: List[CDSTask]) -> List[str]:
        timeout = aiohttp.ClientTimeout(total=None)
        sem = asyncio.Semaphore(3)  # only 3 at a time will call the api
        async with aiohttp.ClientSession(timeout=timeout) as session:

            async def limited_poll(task):
                async with sem:  # only n coroutines enter here concurrently
                    return await self._poll_one(session, task, "./data/raw/cds")

            tasks = [limited_poll(task) for task in cds_tasks]
            results = await asyncio.gather(*tasks)
            return results

    async def _poll_one(
        self, session: aiohttp.ClientSession, task: CDSTask, target_path: str
    ):
        max_retries = 7
        retries = 0
        logger.info("Started polling task %s", task.identifier)
        while retries < max_retries:
            async with session.get(url=task.url, headers=task.headers) as resp:
                response = await resp.json()
                state = response.get("status", "failed")
                if state == "successful":
                    logger.info(
                        "Task %s succeeded - downloading file.", task.identifier
                    )
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

                    file_path = f"{target_path}/{task.identifier}.nc"
                    async with (
                        session.get(url=asset_href, headers=task.headers) as dl,
                        aiofiles.open(file_path, "wb") as f,
                    ):
                        async for chunk in dl.content.iter_chunked(1024):
                            await f.write(chunk)
                    logger.info("Downloaded %s", file_path)
                    return file_path
                elif state == "failed":
                    raise RuntimeError(f"Task failed: {response}")
            retries += 1
            delay = 2 ** (retries + 6) + random.uniform(2, 10)  # eponential waiting
            logger.warning(
                "%s not ready yet — retrying in %.1fs", task.identifier, delay
            )
            await asyncio.sleep(delay)

        logger.error("Task %s timed out after %s retries", task.identifier, max_retries)
        raise TimeoutError(f"Polling timed out after {max_retries} retries")

    def get_data(self, start, end, **kwargs):
        """Fetch and download CDS data for the given time range."""
        cds_tasks = []

        timeframes = self._get_cds_timeframes(start, end)

        batch_size = 10
        batch_delay = 25
        for i in range(0, len(timeframes), batch_size):
            batch = timeframes[i : i + batch_size]
            for timeframe in batch:
                datetime_cds_format = timeframe.to_dict()

                raw_data_location = self.fetcher.fetch(
                    year=datetime_cds_format["year"],
                    month=datetime_cds_format["month"],
                    day=datetime_cds_format["day"],
                    time=datetime_cds_format["time"],
                    **kwargs,
                )
            time.sleep(batch_delay)

            # filename=kwargs.get("filename", f"era5_{datetime_cds_format["year"]}_{datetime_cds_format["month"]}.nc4.nc")
            # print("Querying:")
            # print(datetime_cds_format)
            cds_tasks.append(raw_data_location)
        # print(cds_tasks)
        downloaded_paths = asyncio.run(self._poll_and_download(cds_tasks))

        return downloaded_paths
