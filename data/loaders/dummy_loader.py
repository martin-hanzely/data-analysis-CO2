from __future__ import annotations

from typing import TYPE_CHECKING

from data.loaders.base_loader import BaseLoader
from data.loaders.exceptions import LoaderError

if TYPE_CHECKING:
    from pandas import DataFrame


class DummyLoader(BaseLoader):
    """
    Dummy loader class for testing purposes.
    """
    _df: DataFrame | None = None

    def save_dataframe(self, df: DataFrame) -> None:
        self._df = df

    def retrieve_dataframe(self) -> DataFrame:
        if self._df is None:
            raise LoaderError("Dataframe cannot be retrieved")

        return self._df
