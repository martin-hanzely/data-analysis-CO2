import datetime

import pandas as pd

from data.extractors.opendap_extractor_L2_Standard import OpendapExtractorL2Standard


class TestOpendapExtractorL2Standard:

    def test_get_thredds_catalog_url_for_date(self, test_settings):
        date = datetime.date(2024, 3, 2)  # Day 62
        _e = OpendapExtractorL2Standard(test_settings)
        expected = "https://testbaseurl.com/opendap/OCO2_L2_Standard.11/2024/062/catalog.xml"

        url = _e.get_thredds_catalog_url_for_date(date)

        assert url == expected

    def test_clean_dataframe(self):
        df = pd.DataFrame({
            "xco2": [0.0001, 0.0002, 0.0003],
            "RetrievalResults_outcome_flag": [0, 1, 0]
        })
        expected = pd.DataFrame({"xco2": [200.0]})

        clean_df = OpendapExtractorL2Standard.clean_dataframe(df)
        clean_df.reset_index(inplace=True, drop=True)  # Reset index to compare.

        assert clean_df.equals(expected)
