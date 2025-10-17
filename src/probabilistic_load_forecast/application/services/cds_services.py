import geopandas as gpd
import xarray as xr
import pandas as pd
import regionmask
from probabilistic_load_forecast.adapters import utils

class NoUniqueCountry(Exception):
    """Raised when there is more then one country data in a CDS NetCDF."""

class CreateCDSCountryAverages:
    def __init__(self, cds_repo, db_repo):
        """Fetches and computes the country averages for ERA5 variables
        and stores them into a database."""
        self.cds_repo = cds_repo
        self.db_repo = db_repo

    def __call__(self, start, end):

        # Fetch dataset lazily
        ds: xr.Dataset = self.cds_repo.get(start, end)

        # Compute country averages
        averages_df = self._compute_country_averages(ds)
        
        # Check if there is only one value for the country
        if averages_df["country"].nunique() != 1:
            raise NoUniqueCountry("There are multiple Countries found in the loaded CDS data.")
        
        # Get the country ISO 3166-1 country code
        country = averages_df.iloc[0]["country"]
        norm_country_code = utils.normalize_country(country)
        
        # Remove non ERA5 variable columns
        era5_variables_df = averages_df.drop(columns=["number", "expver", "country"])

        # Store results
        for col_label, content in era5_variables_df.items():
            sub_df = pd.DataFrame(data=content, index=era5_variables_df.index)
            print(sub_df)
            self.db_repo.add(variable=col_label, df=sub_df, stat="instant", country_code=norm_country_code)
            break

    def _compute_country_averages(self, ds: xr.Dataset) -> pd.DataFrame:
        """Aggregate variable means per country, per time step."""

        # Define the country mask using regionmask
        ne_countries = regionmask.defined_regions.natural_earth_v5_0_0.countries_10
        austria_id = ne_countries.map_keys("Austria")
        mask = ne_countries.mask(ds["longitude"], ds["latitude"])
        

        #  Apply mask to the dataset
        austria_mask = mask == austria_id
        masked = ds.where(austria_mask)
        print(ds)

        # Compute the mean over spatial dims
        mean_ds = masked.mean(dim=["latitude", "longitude"])

        # Convert to DataFrame for DB
        df = mean_ds.to_dataframe()
        df["country"] = "Austria"

        return df

class GetERA5DataFromCDSStore():
    def __init__(self, provider):
        self.provider = provider

    def __call__(self, start, end):
        return self.provider.get_data(start, end)