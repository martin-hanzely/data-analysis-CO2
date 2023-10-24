import pandas as pd

from data.loaders.base_loader import BaseLoader
from data.loaders.exceptions import LoaderError


class LocalCSVLoader(BaseLoader):
    """
    Local CSV loader class.
    """
    _out_dir: str = "out/"  # TODO: Use pathlib.
    _file_path: str = _out_dir + "data.csv"  # TODO: Pass file name as parameter to be able to handle multiple files.

    def save_dataframe(self, df: pd.DataFrame) -> None:
        df.to_csv(self._file_path)

    def retrieve_dataframe(self) -> pd.DataFrame:
        try:
            return pd.read_csv(self._file_path)
        except FileNotFoundError:
            raise LoaderError("Dataframe cannot be retrieved")
