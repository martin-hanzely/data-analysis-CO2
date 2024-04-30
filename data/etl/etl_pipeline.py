from __future__ import annotations

import datetime
import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from collections.abc import Iterable

    from data.extractors.base_extractor import BaseExtractor
    from data.loaders.base_loader import BaseLoader


logger = logging.getLogger(__name__)


class ETLPipeline:
    """
    ETL pipeline class.
    """
    _extract_strategy: BaseExtractor
    _load_strategy: BaseLoader

    _dir: str

    def __init__(
            self,
            extract_strategy: BaseExtractor,
            load_strategy: BaseLoader,
            directory: str = ""
    ) -> None:
        """
        Constructor.
        :param extract_strategy:
        :param load_strategy:
        :param directory: The directory to save the data.
        """
        self._extract_strategy = extract_strategy
        self._load_strategy = load_strategy
        self._dir = directory

    def invoke(self, date_range: Iterable[datetime.date]) -> None:
        """
        Invoke the ETL pipeline.
        :param date_range:
        :return:
        """
        for _date, _df in self._extract(date_range):
            try:
                self._load(
                    self._transform(_df),
                    f"{self._dir}/{_date.isoformat()}.gzip"
                )
            except Exception as e:
                # Do not break!
                logger.error("Error processing date %s: %s", _date, e)

    def _extract(self, date_range: Iterable[datetime.date]) -> Iterable[tuple[datetime.date, pd.DataFrame]]:
        yield from self._extract_strategy.extract_date_range(date_range)

    # noinspection PyMethodMayBeStatic
    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def _load(self, df: pd.DataFrame, file_name: str) -> None:
        self._load_strategy.save_dataframe(df, file_name)
