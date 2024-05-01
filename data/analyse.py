from __future__ import annotations

import datetime as dt
import logging
from typing import TYPE_CHECKING

import netCDF4 as nc
import pandas as pd


if TYPE_CHECKING:
    from data.loaders.base_loader import BaseLoader


logger = logging.getLogger(__name__)

GLOBAL_DATE_START = dt.date(2023, 1, 1)


def oco2_daily_avg(loader: BaseLoader) -> None:
    """
    Calculate daily averages for OCO2 data.
    :return:
    """
    date = GLOBAL_DATE_START
    date_stop = dt.date.today()

    # Value lists.
    dates = []
    xco2 = []
    xco2_sk = []
    xco2_eu = []

    while date < date_stop:
        try:
            _df = loader.retrieve_dataframe(f"{date.isoformat()}.gzip")

            # Assign SK attribute for coordinates between extreme points.
            _df["is_sk"] = 0
            _df.loc[
                (_df["latitude"] >= 47.7) & (_df["latitude"] <= 49.6) &
                (_df["longitude"] >= 16.8) & (_df["longitude"] <= 22.6),
                "is_sk"
            ] = 1

            # Assign EU attribute for coordinates between extreme points.
            _df["is_eu"] = 0
            _df.loc[
                (_df["latitude"] >= 36) & (_df["latitude"] <= 71) &
                (_df["longitude"] >= 9) & (_df["longitude"] <= 45),
                "is_eu"
            ] = 1

            avg = _df["xco2"].mean()
            avg_sk = _df[_df["is_sk"] == 1]["xco2"].mean()
            avg_eu = _df[_df["is_eu"] == 1]["xco2"].mean()

            dates.append(date)
            xco2.append(avg)
            xco2_sk.append(avg_sk)
            xco2_eu.append(avg_eu)
        except Exception as exc:
            logger.error(f"Failed to load {date}: {exc}")

        date += dt.timedelta(days=1)

    df = pd.DataFrame({"_date": dates, "xco2": xco2, "xco2_sk": xco2_sk, "xco2_eu": xco2_eu})
    df["_date"] = df["_date"].astype(str)
    loader.save_dataframe(df, "oco2_daily_avg.gzip")


def monthly_avg_per_lat_lon(loader: BaseLoader) -> None:
    """
    Calculate monthly averages for OCO2 data per whole latitude and longitude.
    :return:
    """
    date = GLOBAL_DATE_START
    date_stop = dt.date.today()

    df = pd.DataFrame()
    while date < date_stop:
        try:
            date_df = loader.retrieve_dataframe(f"{date.isoformat()}.gzip")
            df = pd.concat([df, date_df])
        except Exception as exc:
            logger.error(f"Failed to load {date}: {exc}")

        date += dt.timedelta(days=1)

    df["_time"] = pd.to_datetime(df["_time"])
    df["month"] = df["_time"].dt.month
    df["year"] = df["_time"].dt.year
    df["latitude"] = df["latitude"].round()
    df["longitude"] = df["longitude"].round()

    df = df.drop(columns=["_time"])
    df = df.groupby(["year", "month", "latitude", "longitude"]).mean().reset_index()

    loader.save_dataframe(df, "monthly_avg_per_lat_lon.gzip")


def mlo(loader: BaseLoader) -> None:
    """
    Retrieves data from netCDF file and saves it as a dataframe.
    Calculates daily averages for MLO data since 1975.
    :param loader:
    :return:
    """
    mlo_file = "co2_mlo_surface-flask_1_ccgg_event.nc"

    # Get data from MLO file.
    with nc.Dataset(mlo_file, "r") as root_group_MLO:
        _v = root_group_MLO.variables["value"]
        _qc = root_group_MLO.variables["qcflag"]
        retrieval_time_string = nc.chartostring(root_group_MLO.variables["datetime"][:])

        # Create MLO dataframe.
        df = pd.DataFrame({
            "_time": pd.to_datetime(retrieval_time_string, format="%Y-%m-%dT%H:%M:%SZ", utc=True),
            "co2": _v[:],
            "qcflag": _qc[:, 0],
        })

    df = df[df["qcflag"] == b"."]  # Remove invalid data.
    df = df.drop(columns=["qcflag"])

    # Daily averages, year > 1975.
    df = df[df["_time"].dt.year > 1975]
    df["_date"] = df["_time"].dt.date.astype(str)
    df = df.groupby("_date").mean().reset_index()

    # Drop datetime column.
    df = df.drop(columns=["_time"])

    # TODO: Consider returning `df` to ditch `loader` dependency!
    loader.save_dataframe(df, "mlo.gzip")
