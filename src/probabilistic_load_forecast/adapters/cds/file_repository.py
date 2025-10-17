from typing import List
import xarray as xr
from glob import glob
import os


from probabilistic_load_forecast.adapters import utils


class FileRepository:
    def __init__(self, path: str = "data/raw/cds", pattern: str = "*.nc"):
        self.path = path
        self.pattern = pattern

    def _get_dataset(self) -> xr.Dataset:
        """Lazily open all NetCDF files that match the glob pattern."""
        files = sorted(glob(os.path.join(self.path, self.pattern)))
        if not files:
            raise FileNotFoundError(
                f"No files found in {self.path} matching {self.pattern}"
            )

        try:
            dataset = xr.open_mfdataset(paths=files, combine="by_coords", parallel=True)
            return dataset
        except Exception as ex:
            raise IOError(f"Failed to open NetCDF dataset: {ex}") from ex

    def get(self, start, end) -> xr.Dataset:
        """Return a lazily loaded time subset of the dataset."""

        # Failsafe to make sure the datetime is in UTC
        start_utc = utils.to_utc(start)
        end_utc = utils.to_utc(end)
        start_no_tz = utils.remove_tz_info(start_utc)
        end_no_tz = utils.remove_tz_info(end_utc)

        dataset: xr.Dataset = self._get_dataset()

        subset = dataset.sel(valid_time=slice(start_no_tz, end_no_tz))
        return subset

    def add(self, file_paths: List[str]): ...

    def list(self) -> List[str]:
        """List available NetCDF files in the repository."""
        return sorted(glob(os.path.join(self.path, self.pattern)))