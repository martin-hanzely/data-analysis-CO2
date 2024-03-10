from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pandas import DataFrame, Timestamp


class BaseLoader(ABC):
    """
    Abstract base loader class.
    Loader classes are used to save and retrieve dataframes from persistent storage.
    """

    @abstractmethod
    def save_dataframe(self, df: DataFrame) -> None:
        """
        Save dataframe to persistent storage.
        :param df:
        :return: None
        """
        pass

    @abstractmethod
    def retrieve_dataframe(
            self,
            *,
            dt_from: Timestamp,
            dt_to: Timestamp
    ) -> DataFrame:
        """
        Retrieve dataframe records within given range from persistent storage.
        Datetime range boundaries are inclusive.
        :param dt_from:
        :param dt_to:
        :return: Dataframe
        """
        pass
