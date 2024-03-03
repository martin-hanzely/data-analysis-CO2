import pandas as pd
import pytest

from data.loaders.exceptions import LoaderError


class TestDummyLoader:

    @pytest.fixture
    def df(self) -> pd.DataFrame:
        return pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})

    def test_save_dataframe(self, df, dummy_loader):
        dummy_loader.save_dataframe(df)

        assert dummy_loader._df.equals(df)

    def test_retrieve_dataframe(self, df, dummy_loader):
        dummy_loader.save_dataframe(df)

        assert dummy_loader.retrieve_dataframe().equals(df)

    def test_retrieve_dataframe__raises_error_if_no_dataframe(self, dummy_loader):
        assert dummy_loader._df is None, "Test setup is incorrect"

        with pytest.raises(LoaderError):
            dummy_loader.retrieve_dataframe()
