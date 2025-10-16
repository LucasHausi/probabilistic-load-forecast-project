import geopandas as gpd
import xarray as xr
import pandas as pd
import regionmask

class CreateCDSCountryAverages:
    def __init__(self, cds_repo, db_repo):
        """Fetches and computes the country averages for ERA5 variables
        and stores them into a database."""
        self.cds_repo = cds_repo
        self.db_repo = db_repo

    def __call__(self, start, end):
        # 1. Fetch dataset lazily
        ds: xr.Dataset = self.cds_repo.get(start, end)

        # 2. Compute country averages
        averages_df = self._compute_country_averages(ds)
        return averages_df
    
        # 3. Store results
        # self.db_repo.save_country_averages(averages_df)

    def _compute_country_averages(self, ds: xr.Dataset) -> pd.DataFrame:
        """Aggregate variable means per country, per time step."""

        # Define the country mask using regionmask
        ne_countries = regionmask.defined_regions.natural_earth_v5_0_0.countries_110
        austria = ne_countries["Austria"]

        # Create a mask matching your dataset grid
        mask = austria.mask(ds, lon_name="longitude", lat_name="latitude")

        # Apply mask to the dataset
        masked = ds.where(mask == 0)  # region 0 = Austria

        # Compute the mean over spatial dims
        mean_ds = masked.mean(dim=["latitude", "longitude"])

        # Convert to DataFrame for DB
        df = mean_ds.to_dataframe().reset_index()
        df["country"] = "Austria"

        return df
