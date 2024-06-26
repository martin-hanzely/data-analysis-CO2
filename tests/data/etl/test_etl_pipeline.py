import pandas as pd

from data.etl.etl_pipeline import ETLPipeline
from data.extractors.dummy_extractor import DummyExtractor
from data.loaders.dummy_loader import DummyLoader


class TestETLPipeline:

    def test_invoke(self):
        input_df = pd.DataFrame({
            "_time": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T00:01:00Z", "2024-01-01T00:02:00Z"]),
            "latitude": [1.0, 1.5, 48.0],
            "longitude": [1.5, 1.0, 20.0],
            "xco2": [1.0, 2.0, 3.0],
        })
        expected_df = pd.DataFrame({
            "_time": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T00:01:00Z", "2024-01-01T00:02:00Z"]),
            "latitude": [1.0, 1.5, 48.0],
            "longitude": [1.5, 1.0, 20.0],
            "xco2": [1.0, 2.0, 3.0],
        })

        extract_strategy = DummyExtractor(input_df)
        load_strategy = DummyLoader()
        pipeline = ETLPipeline(extract_strategy, load_strategy)

        pipeline.invoke([pd.Timestamp("2024-01-01", tz="UTC").date()])

        output_df = load_strategy.retrieve_dataframe(file_name="2024-01-01.gzip")
        pd.testing.assert_frame_equal(output_df, expected_df, check_like=True)
