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

    def save_dataframe(self, df: pd.DataFrame) -> None:
        df = df.set_index("_time")

        with (
            influxdb.InfluxDBClient(**self._client_kwargs) as _client,
            _client.write_api() as write_client,
        ):
            # noinspection PyTypeChecker
            write_client.write(
                bucket=self._bucket,
                record=df,
                data_frame_measurement_name=self._xco2_measurement_name,
            )

    def retrieve_dataframe(self) -> pd.DataFrame:
        with influxdb.InfluxDBClient(**self._client_kwargs) as _client:
            # TODO: Supply range as an argument!
            query = f"""\
from(bucket: "{self._bucket}")
|> range(start: 2023-12-31T00:00:00Z)
|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
|> keep(columns:["_time", "latitude", "longitude", "xco2"])"""

            df = _client.query_api().query_data_frame(query)
            df = df.drop(columns=["table", "result"])
            return df
