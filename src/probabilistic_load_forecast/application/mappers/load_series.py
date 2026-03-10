import pandas as pd

def load_series_to_dataframe(load_series) -> pd.DataFrame:

    rows = [
        {
            "start_ts": obs.interval.start,
            "end_ts": obs.interval.end,
            "load_mw": obs.load_mw,
        }
        for obs in load_series.observations
    ]

    df = pd.DataFrame(rows, columns=["start_ts", "actual_load_mw"])

    if df.empty:
        return df.set_index(pd.Index([], name="period"))

    df["datetime"] = pd.to_datetime(df["start_ts"], errors="raise")
    df["actual_load_mw"] = pd.to_numeric(df["actual_load_mw"])

    df["period"] = (
        df["datetime"].dt.tz_convert(None).dt.to_period("15min")
    )  # dropping the tz infromation before converting to a period
    df = df[["period","actual_load_mw"]]
    df = df.set_index("period")

    return df
