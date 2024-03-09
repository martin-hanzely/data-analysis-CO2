from pandas import DataFrame

from data.loaders.base_loader import BaseLoader


class InfluxDBLoader(BaseLoader):
    """
    InfluxDB loader class.
    """

    def save_dataframe(self, df: DataFrame) -> None:
        raise NotImplementedError

    def retrieve_dataframe(self) -> DataFrame:
        raise NotImplementedError
