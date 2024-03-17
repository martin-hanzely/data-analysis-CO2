from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from collections.abc import Iterable

    from data.extractors.base_extractor import BaseExtractor
    from data.loaders.base_loader import BaseLoader


class ETLPipeline:
    """
    ETL pipeline class.
    """
    _extract_strategy: BaseExtractor
    _load_strategy: BaseLoader

    _tai93_base_date: pd.Timestamp = pd.Timestamp("1993-01-01", tz="UTC")

    def __init__(self, extract_strategy: BaseExtractor, load_strategy: BaseLoader) -> None:
        self._extract_strategy = extract_strategy
        self._load_strategy = load_strategy

    def invoke(self, date_range: Iterable[datetime.date]) -> None:
        df = self._extract(date_range)
        df = self._transform(df)
        self._load(df)

    def _extract(self, date_range: Iterable[datetime.date]) -> pd.DataFrame:
        dfs = list(self._extract_strategy.extract_date_range(date_range))
        return pd.concat(dfs, ignore_index=True)

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Convert datetime to tai93 to obtain a numeric value
        df["tai93"] = (df["_time"] - self._tai93_base_date).dt.total_seconds()

        # Round coordinates
        df["latitude"] = df["latitude"].apply(lambda x: round(x))
        df["longitude"] = df["longitude"].apply(lambda x: round(x))

        # Aggregate
        df = df.groupby(["latitude", "longitude"]).mean(numeric_only=True).reset_index()

        # Convert tai93 back to datetime
        df["_time"] = self._tai93_base_date + pd.to_timedelta(df["tai93"], unit="s")

        return df.drop(columns=["tai93"])

    def _load(self, df: pd.DataFrame) -> None:
        self._load_strategy.save_dataframe(df)
