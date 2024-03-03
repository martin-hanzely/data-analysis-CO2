from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from data.extractors.base_extractor import BaseExtractor

if TYPE_CHECKING:
    import pandas as pd


class DummyExtractor(BaseExtractor):
    """
    Dummy extractor class for testing purposes.
    """
    _df: pd.DataFrame

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    def extract_for_date(self, date: datetime.date) -> pd.DataFrame:
        return self._df
