from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from abc import ABC, abstractmethod

if TYPE_CHECKING:
    import pandas as pd


class BaseExtractor(ABC):  # TODO: Add method to extract date range.
    """
    Abstract base class for data extractors.
    """
    @abstractmethod
    def extract_for_date(self, date: datetime.date) -> pd.DataFrame:
        """
        Extract data for a specific date.
        :param date: The date to extract data for.
        :return: A DataFrame containing the extracted data.
        """
        pass
