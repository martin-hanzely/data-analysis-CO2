from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

import influxdb_client as influxdb
import pandas as pd

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
    _bucket: str
    _client_kwargs: dict
    _xco2_measurement_name: str = "xco2"

    def __init__(self, settings: Settings) -> None:
        self._bucket = settings.influxdb_bucket
        self._client_kwargs = {
            "url": settings.influxdb_url,
            "token": settings.influxdb_token,
            "org": settings.influxdb_org,
            "debug": settings.debug,
        }

    def save_dataframe(self, df: pd.DataFrame, file_name: str) -> None:
        df = df.set_index("_time")
        df["file_name"] = file_name

        with (
            influxdb.InfluxDBClient(**self._client_kwargs) as _client,
            _client.write_api() as write_client,
        ):
            # noinspection PyTypeChecker
            write_client.write(
                bucket=self._bucket,
                record=df,
                data_frame_measurement_name=self._xco2_measurement_name,
                data_frame_tag_columns=["file_name"],
            )

    def retrieve_dataframe(self, file_name: str) -> pd.DataFrame:
        with influxdb.InfluxDBClient(**self._client_kwargs, timeout=30_000) as _client:
            _dt_format = "%Y-%m-%dT%H:%M:%S.%fZ"
            query = f"""\
from(bucket: "{self._bucket}")
|> filter(fn: (r) => r.file_name == "{file_name}")
|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
|> keep(columns:["_time", "latitude", "longitude", "xco2"])"""

            df = _client.query_api().query_data_frame(query)
            if df.empty:
                return pd.DataFrame(columns=["_time", "latitude", "longitude", "xco2"])

            df = df.drop(columns=["table", "result"])
            return df

    def retrieve_dataframe_for_date_range(
            self,
            *,
            dt_from: pd.Timestamp,
            dt_to: pd.Timestamp
    ) -> pd.DataFrame:
        with influxdb.InfluxDBClient(**self._client_kwargs) as _client:
            _dt_format = "%Y-%m-%dT%H:%M:%S.%fZ"
            query = f"""\
from(bucket: "{self._bucket}")
|> range(start: {dt_from.strftime(_dt_format)}, stop: {dt_to.strftime(_dt_format)})
|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
|> keep(columns:["_time", "latitude", "longitude", "xco2"])"""

            df = _client.query_api().query_data_frame(query)
            if df.empty:
                return pd.DataFrame(columns=["_time", "latitude", "longitude", "xco2"])

            df = df.drop(columns=["table", "result"])
            return df
