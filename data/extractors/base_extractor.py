from __future__ import annotations

import datetime
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    import pandas as pd


class BaseExtractor(ABC):
    """
    Abstract base class for data extractors.
    """
    @abstractmethod
    def extract_date_range(
            self,
            date_range: Iterable[datetime.date]
    ) -> Iterator[tuple[datetime.date, pd.DataFrame]]:
        """
        Extract data for given date range.
        :param date_range: Iterable of dates.
        :return: Iterator of dataframes per date.
        """
        pass
