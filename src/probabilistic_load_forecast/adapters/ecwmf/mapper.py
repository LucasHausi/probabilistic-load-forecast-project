from datetime import datetime
from pathlib import Path

import pandas as pd
import regionmask
import xarray as xr

from probabilistic_load_forecast.domain.model import (
    CountryCode,
    Era5Series,
    InstantWeatherValue,
    IntervalStatistic,
    IntervalWeatherValue,
    Resolution,
    TimeInterval,
    VARIABLE_VALUE_KIND,
    WeatherArea,
    WeatherValueKind,
    WeatherVariable,
)


COUNTRY_NAME_BY_CODE = {
    CountryCode("AT"): "Austria",
}

GRIB_SHORT_NAME_BY_VARIABLE = {
    WeatherVariable.T2M: "2t",
    WeatherVariable.U10: "10u",
    WeatherVariable.V10: "10v",
    WeatherVariable.SSRD: "ssrd",
    WeatherVariable.TP: "tp",
}


class ECWMFMapper:
    def _hourly_instant_values(self, values: pd.Series) -> pd.Series:
        hourly_index = pd.date_range(
            start=values.index.min(),
            end=values.index.max(),
            freq="1h",
            tz="UTC",
        )
        # return the interpolated series (in this case linear as the index is equally spaced)
        return values.reindex(hourly_index).interpolate(method="time")

    def _hourly_interval_values(self, values: pd.Series) -> pd.Series:
        increments = values.diff().dropna()
        hourly_values = {}

        for valid_at, increment in increments.items():
            hour_end = valid_at
            for offset in range(3):
                hourly_values[hour_end - pd.Timedelta(hours=offset)] = float(
                    increment
                ) / 3.0

        result = pd.Series(hourly_values).sort_index()
        result.index = pd.to_datetime(result.index, utc=True)
        return result

    def _trim_instant_values(
        self, values: pd.Series, interval: TimeInterval | None
    ) -> pd.Series:
        if interval is None:
            return values

        return values[(values.index >= interval.start) & (values.index < interval.end)]

    def _open_dataset(self, file_path: str | Path, variable: WeatherVariable) -> xr.Dataset:
        return xr.open_dataset(
            Path(file_path),
            engine="cfgrib",
            backend_kwargs={
                "filter_by_keys": {
                    "shortName": GRIB_SHORT_NAME_BY_VARIABLE[variable],
                },
                # Prevent cfgrib from writing sibling *.idx files. This avoids
                # Windows notebook issues with missing/locked index files.
                "indexpath": "",
            },
        )

    def _prepare_dataset(
        self, file_paths: list[str], variable: WeatherVariable
    ) -> xr.Dataset:
        """This function loads and concatenates the datasets for a single
        variable.

        Args:
            file_paths (list[str]): The paths of the .grib2 files to load
            variable (WeatherVariable): The variable to process.

        Returns:
            xr.Dataset: A concatenated dataset along the valid_time dim.
        """
        datasets = []
        
        # load and collect all datasets for the variable
        # for the given file paths
        for file_path in file_paths:
            ds = self._open_dataset(file_path, variable)
            if "step" in ds.dims:
                ds = ds.swap_dims({"step": "valid_time"})
            ds = ds.sortby("valid_time")
            datasets.append(ds)

        # concatenate all datasets along the valid time dim
        dataset = xr.concat(
            datasets,
            dim="valid_time",
            coords="minimal",
            compat="override",
        ).sortby("valid_time")
        valid_time_index = dataset.indexes["valid_time"]
        # filter out the duplicate entries in the array and return the result
        return dataset.isel(valid_time=~valid_time_index.duplicated())

    def _country_mask(self, ds: xr.Dataset, area: WeatherArea) -> xr.DataArray:
        country_name = COUNTRY_NAME_BY_CODE.get(area.code)
        if country_name is None:
            raise ValueError(f"unsupported weather area: {area.code}")

        countries = regionmask.defined_regions.natural_earth_v5_0_0.countries_10
        country_id = countries.map_keys(country_name)
        mask = countries.mask(ds["longitude"], ds["latitude"], flag=None)
        return mask == country_id

    def _country_mean(
        self, ds: xr.Dataset, variable: WeatherVariable, area: WeatherArea
    ) -> pd.Series:
        mask = self._country_mask(ds, area)
        averaged = ds.where(mask).mean(dim=["latitude", "longitude"])

        data_array = averaged[variable.value].to_series()
        data_array.index = pd.to_datetime(data_array.index, utc=True)
        return data_array.sort_index()

    def _map_instant_observations(
        self, values: pd.Series, area: WeatherArea, variable: WeatherVariable
    ) -> tuple[InstantWeatherValue, ...]:
        return tuple(
            InstantWeatherValue(
                area=area,
                variable=variable,
                valid_at=valid_at.to_pydatetime(),
                value=float(value),
            )
            for valid_at, value in values.items()
        )

    def _map_interval_observations(
        self, values: pd.Series, area: WeatherArea, variable: WeatherVariable
    ) -> tuple[IntervalWeatherValue, ...]:
        return tuple(
            IntervalWeatherValue(
                area=area,
                variable=variable,
                interval=TimeInterval(
                    start=(valid_at - pd.Timedelta(hours=1)).to_pydatetime(),
                    end=valid_at.to_pydatetime(),
                ),
                statistic=IntervalStatistic.TOTAL,
                value=float(value),
            )
            for valid_at, value in values.items()
        )

    def map(
        self,
        file_paths: list[str],
        *,
        area: WeatherArea,
        weather_variable: WeatherVariable,
        interval: TimeInterval | None = None,
    ) -> Era5Series:
        ds = self._prepare_dataset(file_paths, weather_variable)
        value_kind = VARIABLE_VALUE_KIND[weather_variable]

        # if interval is not None and value_kind is not WeatherValueKind.INSTANT:
        #     valid_time = pd.to_datetime(ds["valid_time"].values, utc=True)
        #     mask = (valid_time >= interval.start) & (valid_time < interval.end)
        #     ds = ds.isel(valid_time=mask)

        # create a country mask and average over position dimensions
        values = self._country_mean(ds, weather_variable, area)

        if value_kind is WeatherValueKind.INSTANT:
            values = self._hourly_instant_values(values)
            values = self._trim_instant_values(values, interval)
            observations = self._map_instant_observations(
                values=values,
                area=area,
                variable=weather_variable,
            )
        else:
            # convert 3-hourly accumulated values into 1-hour values
            values = self._hourly_interval_values(values)
            observations = self._map_interval_observations(
                values=values,
                area=area,
                variable=weather_variable,
            )

        return Era5Series(
            area=area,
            resolution=Resolution.PT1H,
            observations=observations,
            variable=weather_variable,
        )
