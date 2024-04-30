import datetime
import logging

import netCDF4 as nc
import pandas as pd

from data.loaders.local_parquet_loader import LocalParquetLoader


logger = logging.getLogger(__name__)


def oco2_daily_avg():
    """
    Calculate daily averages for OCO2 data.
    :return:
    """
    loader = LocalParquetLoader()

    date = datetime.date(2023, 1, 1)
    stop_date = datetime.date(2024, 3, 1)

    dates = []
    averages = []
    averages_sk = []
    averages_eu = []
    while date < stop_date:
        try:
            date_df = loader.retrieve_dataframe(f"out_/{date.isoformat()}.gzip")

            # Assign country Slovakia flag between extreme points.
            date_df['sk_flag'] = 0
            date_df.loc[
                (date_df["latitude"] >= 47.7) & (date_df["latitude"] <= 49.6) &
                (date_df["longitude"] >= 16.8) & (date_df["longitude"] <= 22.6),
                'sk_flag'
            ] = 1

            # Assign EU flag between extreme points.
            date_df['eu_flag'] = 0
            date_df.loc[
                (date_df["latitude"] >= 36) & (date_df["latitude"] <= 71) &
                (date_df["longitude"] >= 9) & (date_df["longitude"] <= 45),
                'eu_flag'
            ] = 1

            avg = date_df['xco2'].mean()
            avg_sk = date_df[date_df['sk_flag'] == 1]['xco2'].mean()
            avg_eu = date_df[date_df['eu_flag'] == 1]['xco2'].mean()

            dates.append(date)
            averages.append(avg)
            averages_sk.append(avg_sk)
            averages_eu.append(avg_eu)
        except Exception as exc:
            logger.error(f'Failed to load {date}: {exc}')

        date += datetime.timedelta(days=1)

    df = pd.DataFrame({'date': dates, 'average': averages, 'average_sk': averages_sk, 'average_eu': averages_eu})
    df['date'] = df['date'].astype(str)
    loader.save_dataframe(df, 'out_/oco2_daily_avg.gzip')


def monthly_avg_per_lat_lon():
    """
    Calculate monthly averages for OCO2 data per whole latitude and longitude.
    :return:
    """
    loader = LocalParquetLoader()

    date = datetime.date(2023, 1, 1)
    stop_date = datetime.date(2024, 3, 1)

    df = pd.DataFrame()
    while date < stop_date:
        try:
            date_df = loader.retrieve_dataframe(f"out_/{date.isoformat()}.gzip")
            df = pd.concat([df, date_df])
        except Exception as exc:
            logger.error(f'Failed to load {date}: {exc}')

        date += datetime.timedelta(days=1)

    df['_time'] = pd.to_datetime(df['_time'])
    df['month'] = df['_time'].dt.month
    df['year'] = df['_time'].dt.year
    df['latitude'] = df['latitude'].round()
    df['longitude'] = df['longitude'].round()

    df = df.drop(columns=['_time'])
    df = df.groupby(['year', 'month', 'latitude', 'longitude']).mean().reset_index()

    loader.save_dataframe(df, 'out_/monthly_avg_per_lat_lon.gzip')


def monthly_avg_sk():
    """
    Calculate monthly averages for OCO2 data per Slovakia.
    :return:
    """
    loader = LocalParquetLoader()

    date = datetime.date(2023, 1, 1)
    stop_date = datetime.date(2024, 3, 1)

    df = pd.DataFrame()
    while date < stop_date:
        try:
            date_df = loader.retrieve_dataframe(f"out_/{date.isoformat()}.gzip")
            # Assign country Slovakia flag between extreme points.
            date_df['sk_flag'] = 0
            date_df.loc[
                (date_df["latitude"] >= 47.7) & (date_df["latitude"] <= 49.6) &
                (date_df["longitude"] >= 16.8) & (date_df["longitude"] <= 22.6),
                'sk_flag'
            ] = 1

            df = pd.concat([df, date_df[date_df['sk_flag'] == 1]])
        except Exception as exc:
            logger.error(f'Failed to load {date}: {exc}')

        date += datetime.timedelta(days=1)

    df['_time'] = pd.to_datetime(df['_time'])
    df['month'] = df['_time'].dt.month
    df['year'] = df['_time'].dt.year
    df['latitude'] = df['latitude'].round(3)
    df['longitude'] = df['longitude'].round(3)

    df = df.drop(columns=['_time'])
    df = df.groupby(['year', 'month', 'latitude', 'longitude']).mean().reset_index()

    loader.save_dataframe(df, 'out_/monthly_avg_sk.gzip')


def mlo():
    loader = LocalParquetLoader()

    _mlo_file = "out_/co2_mlo_surface-flask_1_ccgg_event.nc"

    # Get data from MLO file.
    with nc.Dataset(_mlo_file, "r") as root_group_MLO:
        _v = root_group_MLO.variables["value"]
        _qc = root_group_MLO.variables["qcflag"]
        retrieval_time_string = nc.chartostring(root_group_MLO.variables["datetime"][:])

        # Create MLO dataframe.
        df_MLO = pd.DataFrame({
            "datetime": pd.to_datetime(retrieval_time_string, format="%Y-%m-%dT%H:%M:%SZ", utc=True),
            "value": _v[:],
            "qcflag": _qc[:, 0],
        })

    df_MLO = df_MLO[df_MLO["qcflag"] == b"."]  # Remove invalid data.
    df_MLO = df_MLO.drop(columns=["qcflag"])

    # Daily averages, year > 1975.
    df_MLO = df_MLO[df_MLO["datetime"].dt.year > 1975]
    df_MLO["date"] = df_MLO["datetime"].dt.date.astype(str)
    df_MLO = df_MLO.groupby("date").mean().reset_index()

    # Drop datetime column.
    df_MLO = df_MLO.drop(columns=["datetime"])

    loader.save_dataframe(df_MLO, "out_/mlo.gzip")
