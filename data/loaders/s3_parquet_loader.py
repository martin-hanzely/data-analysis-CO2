from __future__ import annotations

import contextlib
import os
import tempfile
from typing import TYPE_CHECKING, IO

import pandas as pd

from data.loaders.base_loader import BaseLoader
from data.services.aws_s3 import S3Service

if TYPE_CHECKING:
    from data.settings import Settings


class S3ParquetLoader(BaseLoader):
    """
    S3 Parquet loader class.
    """
    _s3_service: S3Service

    def __init__(self, settings: Settings) -> None:
        self._s3_service = S3Service(settings=settings)

    def save_dataframe(self, df: pd.DataFrame, file_name: str) -> None:
        # In memory IO buffer raises `ValueError: write on closed file`.
        with self.closed_named_temporary_file() as _f:
            df.to_parquet(_f.name, engine="fastparquet", compression="gzip", index=False)

            with open(_f.name, "rb") as _f0:
                self._s3_service.upload_file_obj(_f0, file_name)

    def retrieve_dataframe(self, file_name: str) -> pd.DataFrame:
        # In memory IO buffer raises `ValueError: read on closed file`.
        with self.closed_named_temporary_file() as _f:
            with open(_f.name, "wb") as _f0:
                self._s3_service.download_file_obj(_f0, file_name)

            return pd.read_parquet(_f.name, engine="fastparquet")

    @contextlib.contextmanager
    def closed_named_temporary_file(self) -> IO[bytes]:
        _f = tempfile.NamedTemporaryFile(delete=False)
        _f.close()
        try:
            yield _f
        finally:
            _f.close()
            os.unlink(_f.name)
