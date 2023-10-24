from data.etl.etl_pipeline import ETLPipeline


class TestETLPipeline:

    def test_invoke(self, dummy_loader):
        etl_pipeline = ETLPipeline(load_strategy=dummy_loader)
        etl_pipeline.invoke()

        df = dummy_loader.retrieve_dataframe()
        assert df is not None
        assert df.columns.tolist() == ["year", "value"]
        # No assertions on actual data as it is loaded from external source.
