from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from data.extractors.base_extractor import BaseExtractor

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    import pandas as pd


class DummyExtractor(BaseExtractor):
    """
    Dummy extractor class for testing purposes.
    """
    _df: pd.DataFrame

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    def extract_date_range(
            self,
            date_range: Iterable[datetime.date]
    ) -> Iterator[pd.DataFrame]:
        yield self._df
