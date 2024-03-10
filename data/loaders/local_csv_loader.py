from __future__ import annotations

import io
from typing import IO

import pandas as pd

from data.loaders.base_loader import BaseLoader
from data.loaders.exceptions import LoaderError


class LocalCSVLoader(BaseLoader):
    """
    Local CSV loader class.
    """
    _in_memory: bool
    _path_or_buf: IO[str] | str
    _out_dir: str = "out/"

    def __init__(self, in_memory: bool = False) -> None:
        self._in_memory = in_memory
        if in_memory:
            self._path_or_buf = io.StringIO()
        else:
            self._path_or_buf = self._out_dir + "data.csv"

    def save_dataframe(self, df: pd.DataFrame) -> None:
        df.to_csv(self._path_or_buf, index=False)

    def retrieve_dataframe(
            self,
            *,
            dt_from: pd.Timestamp,
            dt_to: pd.Timestamp
    ) -> pd.DataFrame:
        if self._in_memory:
            self._path_or_buf.seek(0)

        try:
            df = pd.read_csv(self._path_or_buf, parse_dates=["_time"])
            return df[df["_time"].between(dt_from, dt_to, inclusive="both")]
        except FileNotFoundError:
            raise LoaderError("Dataframe cannot be retrieved")
