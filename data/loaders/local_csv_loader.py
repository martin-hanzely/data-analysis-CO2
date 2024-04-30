from __future__ import annotations

import io
from typing import IO

import pandas as pd

from data.loaders.base_loader import BaseLoader


class LocalCSVLoader(BaseLoader):
    """
    Local CSV loader class.
    """
    _in_memory: bool
    _buf: IO[str] | None = None

    def __init__(self, in_memory: bool = False) -> None:
        self._in_memory = in_memory
        if in_memory:
            self._buf = io.StringIO()

    def save_dataframe(self, df: pd.DataFrame, file_name: str) -> None:
        if self._in_memory:
            if self._buf is None:
                raise ValueError("Buffer is not initialized")

            df.to_csv(self._buf, index=False)
            return

        df.to_csv(file_name, index=False)

    def retrieve_dataframe(self, file_name: str) -> pd.DataFrame:
        if self._in_memory:
            if self._buf is None:
                raise ValueError("Buffer is not initialized")

            self._buf.seek(0)
            return pd.read_csv(self._buf, parse_dates=["_time"])

        return pd.read_csv(file_name, parse_dates=["_time"])
