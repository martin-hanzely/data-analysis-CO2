import enum

from data.extractors.base_extractor import BaseExtractor
from data.extractors.opendap_extractor_L2_Lite_FP import OpendapExtractorL2LiteFP
from data.extractors.opendap_extractor_L2_Standard import OpendapExtractorL2Standard


class ExtractorChoices(str, enum.Enum):
    """
    Enum for the available extractors.
    """
    opendap_L2_Standard = "opendap_L2_Standard"
    opendap_L2_Lite_FP = "opendap_L2_Lite_FP"


def get_extractor_class(extractor_choice: ExtractorChoices | str) -> type[BaseExtractor]:
    """
    Get the extractor class for the given choice.
    :param extractor_choice: ExtractorChoices
    :return: Extractor class
    """
    _map = {
        ExtractorChoices.opendap_L2_Standard: OpendapExtractorL2Standard,
        ExtractorChoices.opendap_L2_Lite_FP: OpendapExtractorL2LiteFP,
    }

    try:
        return _map[ExtractorChoices(extractor_choice)]
    except (KeyError, ValueError) as e:
        raise ValueError(f"Invalid extractor choice: {extractor_choice}") from e
