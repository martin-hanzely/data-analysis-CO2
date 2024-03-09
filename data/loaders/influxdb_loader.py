from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

import influxdb_client as influxdb
import pandas as pd
from influxdb_client.client.write_api import SYNCHRONOUS

from data.loaders.base_loader import BaseLoader

if TYPE_CHECKING:
    from data.settings import Settings


class InfluxDBClientKwargs(TypedDict):
    url: str
    token: str
    org: str
    debug: bool | None


class InfluxDBLoader(BaseLoader):
    """
    InfluxDB loader class.
    """
    _client_kwargs: dict
    _settings: Settings
    _default_bucket: str = "xco2"  # TODO: Move to settings!

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client_kwargs = {
            "url": settings.influxdb_url,
            "token": settings.influxdb_token,
            "org": settings.influxdb_org,
            "debug": settings.debug,
        }

    def save_dataframe(self, df: pd.DataFrame) -> None:
        df["_time"] = df["datetime"]
        df = df.drop(columns=["datetime"])
        df = df.set_index("_time")

        with (
            influxdb.InfluxDBClient(**self._client_kwargs) as _client,
            _client.write_api(write_options=SYNCHRONOUS) as write_client,
        ):
            # noinspection PyTypeChecker
            write_client.write(
                bucket=self._default_bucket,
                record=df,
                data_frame_measurement_name="xco2",
            )

    def retrieve_dataframe(self) -> pd.DataFrame:
        with (
            influxdb.InfluxDBClient(**self._client_kwargs) as _client,
            _client.query_api() as query_client,
        ):
            query = f"""\
from(bucket: "{self._default_bucket}")
|> keep(columns: ["_time", "latitude", "longitude", "xco2"])"""
            df = query_client.query_data_frame(query)
            df["datetime"] = df["_time"]
            df = df.drop(columns=["_time"])
            return df
