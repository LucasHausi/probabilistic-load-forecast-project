"""Services for handling CDS data."""

import xarray as xr
import pandas as pd
import regionmask
from probabilistic_load_forecast.application.ports import CountryCodeNormalizer

from probabilistic_load_forecast.domain.model import (
    TimeInterval,
    WeatherArea,
    WeatherVariable,
    Era5Series
)

from probabilistic_load_forecast.adapters.db import(
    Era5PostgreRepository
)

from probabilistic_load_forecast.application.mappers import (
    era5_series_to_dataframe
)

STAT_BY_VAR: dict = {
    "ssrd": "integrated_flux",  # J/m² over (t-1h, t]
    "tp": "integrated_flux",  # (t-1h, t]
    "t2m": "instant",
    "u10": "instant",
    "v10": "instant",
}


class NoUniqueCountry(Exception):
    """Raised when there is more then one country data in a CDS NetCDF."""


class CreateCDSCountryAverages:
    """Fetches ERA5 data from CDS repository, computes country averages,
    and stores them into a database."""

    def __init__(self, cds_repo, db_repo: Era5PostgreRepository, country_code_normalizer: CountryCodeNormalizer):
        """Fetches and computes the country averages for ERA5 variables
        and stores them into a database."""
        self.cds_repo = cds_repo
        self.db_repo = db_repo
        self.country_code_normalizer = country_code_normalizer

    def __call__(self, interval: TimeInterval):

        # Fetch dataset lazily
        ds: xr.Dataset = self.cds_repo.get(interval.start, interval.end)

        # Compute country averages
        averages_df = self._compute_country_averages(ds)

        # Check if there is only one value for the country
        if averages_df["country"].nunique() != 1:
            raise NoUniqueCountry(
                "There are multiple Countries found in the loaded CDS data."
            )

        # Get the country ISO 3166-1 country code
        country = averages_df.iloc[0]["country"]
        country_code = self.country_code_normalizer.normalize(country)

        # Remove non ERA5 variable columns
        era5_variables_df = averages_df.drop(columns=["number", "expver", "country"])

        # Store results
        for col_label, content in era5_variables_df.items():
            content.name = "value"
            sub_df = pd.DataFrame(data=content, index=era5_variables_df.index)

            self.db_repo.add(
                variable=col_label,
                df=sub_df,
                stat=STAT_BY_VAR.get(col_label, "instant"),
                country_code=country_code.value,
            )

    def _convert_accumulated_to_hourly(
        self, ds: xr.Dataset, variables: list[str]
    ) -> xr.Dataset:
        for variable in variables:
            acc = ds[variable]
            diff_array = acc.diff("valid_time")
            variable_hourly = diff_array.where(diff_array >= 0, 0.0)
            variable_hourly = variable_hourly.assign_coords(
                valid_time=acc.valid_time[1:]
            )
            ds[variable] = variable_hourly
        return ds

    def _compute_country_averages(self, ds: xr.Dataset) -> pd.DataFrame:
        """Aggregate variable means per country, per time step."""

        # Define the country mask using regionmask
        ne_countries = regionmask.defined_regions.natural_earth_v5_0_0.countries_10
        austria_id = ne_countries.map_keys("Austria")
        mask = ne_countries.mask(ds["longitude"], ds["latitude"])

        # Apply mask to the dataset
        austria_mask = mask == austria_id
        masked = ds.where(austria_mask)

        # Compute the mean over spatial dims
        ds_proc = ds.copy()
        ds_proc = self._convert_accumulated_to_hourly(ds_proc, ["ssrd", "tp"])
        masked = ds_proc.where(austria_mask)
        mean_ds_hourly = masked.mean(dim=["latitude", "longitude"])

        # Convert to DataFrame for DB
        df = mean_ds_hourly.to_dataframe()
        df["country"] = "Austria"

        return df


class GetERA5DataFromCDSStore:
    """Service to get ERA5 data from CDS data provider."""

    def __init__(self, provider):
        self.provider = provider

    def __call__(self, interval: TimeInterval):
        return self.provider.get_data(interval.start, interval.end)


class GetMultipleERA5DataFrameFromDB:
    """Use case that retrieves actual load data from a repository."""

    def __init__(self, repo):
        self.repo = repo

    def __call__(
        self,
        variables: list[WeatherVariable],
        area: WeatherArea,
        interval: TimeInterval,
        schema: str = "public",
    ) -> dict[WeatherVariable, pd.DataFrame]:
        """
        Fetch multiple time series datasets (one per variable) for a country.

        Returns a dict: {variable_name: DataFrame}
        """
        results = {}
        for variable in variables:
            try:
                series = self.repo.get(
                    interval,
                    area=area,
                    variable=variable,
                    schema=schema,
                )

                results[variable] = era5_series_to_dataframe(series)
            except Exception as e:
                print(f"Could not fetch {variable}: {e}")
        return results

class GetERA5DataFromDB:
    """Use case that retrieves actual load data from a repository."""

    def __init__(self, repo: Era5PostgreRepository):
        self.repo = repo

    def __call__(
        self,
        variable: WeatherVariable,
        area: WeatherArea,
        interval: TimeInterval,
        schema: str = "public",
    ) -> Era5Series:
        """
        Fetch multiple time series datasets (one per variable) for a country.

        Returns a dict: {variable_name: DataFrame}
        """

        return self.repo.get(
            interval,
            area,
            variable,
            schema=schema
        )
