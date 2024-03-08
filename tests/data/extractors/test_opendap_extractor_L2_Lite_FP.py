import datetime

import pandas as pd

from data.extractors.opendap_extractor_L2_Lite_FP import OpendapExtractorL2LiteFP


class TestOpendapExtractorL2LiteFP:

    def test_get_thredds_catalog_url_for_year(self, test_settings):
        year = 2024
        _e = OpendapExtractorL2LiteFP(test_settings)
        expected = "https://testbaseurl.com/opendap/OCO2_L2_Lite_FP.11.1r/2024/catalog.xml"

        url = _e.get_thredds_catalog_url_for_year(year)

        assert url == expected

    def test_clean_dataframe(self):
        df = pd.DataFrame({"a": [1, 2, 3], "xco2_quality_flag": [0, 1, 0]})
        expected = pd.DataFrame({"a": [1, 3]})

        clean_df = OpendapExtractorL2LiteFP.clean_dataframe(df)
        clean_df.reset_index(inplace=True, drop=True)  # Reset index to compare.

        assert clean_df.equals(expected)
