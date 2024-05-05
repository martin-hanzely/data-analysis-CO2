from __future__ import annotations

import pandas as pd

from data.loaders.base_loader import BaseLoader
from data.loaders.exceptions import LoaderError


class DummyLoader(BaseLoader):
    """
    Dummy loader class for testing purposes.
    """
    _df: pd.DataFrame | None = None

    def save_dataframe(self, df: pd.DataFrame, file_name: str) -> None:
        if self._df is None:
            self._df = df
        else:
            self._df = pd.concat([self._df, df], ignore_index=True, sort=False)

    def retrieve_dataframe(self, file_name: str) -> pd.DataFrame:
        if self._df is None:
            raise LoaderError("Dataframe cannot be retrieved")

        return self._df
