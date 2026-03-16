import pandas as pd

from probabilistic_load_forecast.domain.model import (
    Era5Series,
    InstantWeatherValue,
)


def era5_series_to_dataframe(series: Era5Series):
    rows = [
        {
            "valid_time": (
                obs.valid_at
                if isinstance(obs, InstantWeatherValue)
                else obs.interval.start
            ),
            "value": obs.value,
        }
        for obs in series.observations
    ]

    df = pd.DataFrame(data=rows)
    df.set_index("valid_time", inplace=True) 
    return df


