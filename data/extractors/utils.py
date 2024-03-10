import enum

from data.extractors.base_opendap_extractor import BaseOpendapExtractor
from data.extractors.opendap_extractor_L2_Lite_FP import OpendapExtractorL2LiteFP
from data.extractors.opendap_extractor_L2_Standard import OpendapExtractorL2Standard


class OpendapExtractorChoices(str, enum.Enum):
    """
    Enum for the available OPeNDAP extractors.
    """
    opendap_L2_Standard = "opendap_L2_Standard"
    opendap_L2_Lite_FP = "opendap_L2_Lite_FP"


def get_opendap_extractor_class(extractor_choice: OpendapExtractorChoices | str) -> type[BaseOpendapExtractor]:
    """
    Get OPeNDAP extractor class for the given choice.
    :param extractor_choice: ExtractorChoices
    :return: Extractor class
    """
    _map = {
        OpendapExtractorChoices.opendap_L2_Standard: OpendapExtractorL2Standard,
        OpendapExtractorChoices.opendap_L2_Lite_FP: OpendapExtractorL2LiteFP,
    }

    try:
        return _map[OpendapExtractorChoices(extractor_choice)]
    except (KeyError, ValueError) as e:
        raise ValueError(f"Invalid extractor choice: {extractor_choice}") from e
