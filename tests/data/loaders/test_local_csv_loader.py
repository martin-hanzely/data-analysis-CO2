import datetime

import pandas as pd
import pytest

from data.loaders.local_csv_loader import LocalCSVLoader


class TestLocalCSVLoader:
    @pytest.fixture
    def local_csv_loader(self) -> LocalCSVLoader:
        return LocalCSVLoader(in_memory=True)

    # noinspection DuplicatedCode
    def test_loader_workflow(self, dummy_df, local_csv_loader):
        assert isinstance(dummy_df, pd.DataFrame) and len(dummy_df) > 0  # Sanity check

        expected_df = pd.DataFrame({
            "_time": pd.to_datetime(["2024-01-01T02:01"], utc=True),
            "latitude": [0.0],
            "longitude": [0.0],
            "xco2": [420.0],
        })

        local_csv_loader.save_dataframe(dummy_df)
        loaded_df = local_csv_loader.retrieve_dataframe(
            dt_from=pd.to_datetime("2024-01-01T01:30", utc=True),
            dt_to=pd.to_datetime("2024-01-01T02:30", utc=True),
        )
        loaded_df = loaded_df.reset_index(drop=True)

        pd.testing.assert_frame_equal(loaded_df, expected_df)
