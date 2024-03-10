import pytest

from data.extractors.opendap_extractor_L2_Lite_FP import OpendapExtractorL2LiteFP
from data.extractors.opendap_extractor_L2_Standard import OpendapExtractorL2Standard
from data.extractors.utils import OpendapExtractorChoices, get_opendap_extractor_class


@pytest.mark.parametrize(
    "extractor_choice,expected",
    [
        ("opendap_L2_Standard", OpendapExtractorL2Standard),
        (OpendapExtractorChoices.opendap_L2_Lite_FP, OpendapExtractorL2LiteFP),
        pytest.param("unknown", None, marks=pytest.mark.xfail(raises=ValueError)),
    ],
)
def test_get_opendap_extractor_class(extractor_choice, expected):
    assert get_opendap_extractor_class(extractor_choice) is expected
