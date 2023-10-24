from __future__ import annotations

from typing import TYPE_CHECKING

from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from pandas import DataFrame


class BaseLoader(ABC):
    """
    Abstract base loader class.
    Loader classes are used to save and retrieve dataframes from persistent storage.
    """

    @abstractmethod
    def save_dataframe(self, df: DataFrame) -> None:
        """
        Save dataframe to persistent storage.
        :param df: Dataframe to save
        :return: None
        """
        pass

    @abstractmethod
    def retrieve_dataframe(self) -> DataFrame:
        """
        Retrieve dataframe from persistent storage.
        :return: Dataframe
        """
        pass
