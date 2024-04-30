from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


class BaseLoader(ABC):
    """
    Abstract base loader class.
    Loader classes are used to save and retrieve dataframes from persistent storage.
    """

    @abstractmethod
    def save_dataframe(self, df: pd.DataFrame, file_name: str) -> None:
        """
        Save dataframe to persistent storage.
        :param df:
        :param file_name:
        :return: None
        """
        pass

    @abstractmethod
    def retrieve_dataframe(self, file_name: str) -> pd.DataFrame:
        """
        Retrieve dataframe from persistent storage.
        :param file_name:
        :return: Dataframe
        """
        pass
