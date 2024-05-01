import logging

import pandas as pd

from data.conf import get_app_settings
from data.loaders.influxdb_loader import InfluxDBLoader
from data.loaders.s3_parquet_loader import S3ParquetLoader

logger = logging.getLogger(__name__)


def move_influxdb_to_s3():
    """
    Move InfluxDB data to S3
    """
    settings = get_app_settings()
    influxdb_loader = InfluxDBLoader(settings)
    s3_loader = S3ParquetLoader(settings)

    date = pd.Timestamp("2024-03-01T00:00:00Z")
    date_stop = pd.Timestamp("2024-04-30T00:00:00Z")

    while date < date_stop:
        dt_to = date + pd.Timedelta(days=1)
        try:
            logger.info(f"Moving {date}")
            df = influxdb_loader.retrieve_dataframe_for_date_range(dt_from=date, dt_to=dt_to)
            s3_loader.save_dataframe(df, f"{date.date().isoformat()}.gzip")
        except Exception as exc:
            logger.error(f"Failed to move {date}: {exc}")

        date = dt_to
