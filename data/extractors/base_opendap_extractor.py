from __future__ import annotations

import logging
from abc import ABC
from typing import TYPE_CHECKING

from data.extractors.base_extractor import BaseExtractor
from data.utils.opendap import OpendapClient

if TYPE_CHECKING:
    from data.settings import Settings


logger = logging.getLogger(__name__)


class BaseOpendapExtractor(BaseExtractor, ABC):
    """
    Base class for OpenDAP extractors with defined constructor dependencies.
    """
    _settings: Settings
    _client: OpendapClient

    def __init__(self, settings: Settings, client: OpendapClient) -> None:
        self._settings = settings
        self._client = client
