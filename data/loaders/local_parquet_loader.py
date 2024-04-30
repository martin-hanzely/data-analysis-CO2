from __future__ import annotations

import pandas as pd

from data.loaders.base_loader import BaseLoader


class LocalParquetLoader(BaseLoader):
    """
    Local Parquet loader class.
    """

    def save_dataframe(self, df: pd.DataFrame, file_name: str) -> None:
        df.to_parquet(file_name, engine="fastparquet", compression="gzip", index=False)

    def retrieve_dataframe(self, file_name: str) -> pd.DataFrame:
        return pd.read_parquet(file_name, engine="fastparquet")
