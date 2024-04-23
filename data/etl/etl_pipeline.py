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

    # _tai93_base_date: pd.Timestamp = pd.Timestamp("1993-01-01", tz="UTC")

    def __init__(self, extract_strategy: BaseExtractor, load_strategy: BaseLoader) -> None:
        self._extract_strategy = extract_strategy
        self._load_strategy = load_strategy

    def invoke(self, date_range: Iterable[datetime.date]) -> None:
        for _df in self._extract(date_range):
            self._load(self._transform(_df))

    def _extract(self, date_range: Iterable[datetime.date]) -> Iterable[pd.DataFrame]:
        yield from self._extract_strategy.extract_date_range(date_range)

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Create column of null values
        df["country"] = "NA"
        # Assign country tag to the points within the extreme points of Slovakia
        df.loc[
            (df["latitude"] >= 47.7) & (df["latitude"] <= 49.6) &
            (df["longitude"] >= 16.8) & (df["longitude"] <= 22.6),
            "country"
        ] = "SK"

        """
        # Convert datetime to tai93 to obtain a numeric value
        df["tai93"] = (df["_time"] - self._tai93_base_date).dt.total_seconds()
        # Round coordinates
        df["latitude"] = df["latitude"].apply(lambda x: round(x))
        df["longitude"] = df["longitude"].apply(lambda x: round(x))
        # Aggregate
        df = df.groupby(["latitude", "longitude"]).mean(numeric_only=True).reset_index()
        # Convert tai93 back to datetime
        df["_time"] = self._tai93_base_date + pd.to_timedelta(df["tai93"], unit="s")
        """
        return df

    def _load(self, df: pd.DataFrame) -> None:
        self._load_strategy.save_dataframe(df)
