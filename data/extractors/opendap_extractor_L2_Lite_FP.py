from __future__ import annotations

import datetime
import logging
import tempfile
import os
from abc import ABC
from typing import TYPE_CHECKING

# noinspection PyPep8Naming
import netCDF4 as nc
import pandas as pd
import requests

from data.extractors.base_extractor import BaseExtractor
from data.utils.thredds_catalog import (
    THREDDSCatalogError,
    get_thredds_catalog_xml,
    get_opendap_urls,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from data.settings import Settings


logger = logging.getLogger(__name__)


class OpendapExtractorL2LiteFP(BaseExtractor):
    """
    Extractor class for data from the NASA Earth Data GES DISC OPeNDAP server.
    """
    _settings: Settings

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def extract_date_range(self, date_range: Iterable[datetime.date]) -> Iterator[pd.DataFrame]:
        date_list = list(date_range)
        if len(date_list) < 1:
            yield from ()

        base_year = date_list[0].year
        try:
            catalog_url = self.get_thredds_catalog_url_for_year(base_year)
            catalog_xml = get_thredds_catalog_xml(catalog_url)
            opendap_urls = list(get_opendap_urls(
                catalog_xml,
                file_suffix=".nc4",
                variables=[
                    "xco2",
                    "time",
                    "longitude",
                    "latitude",
                    "xco2_quality_flag",
                ]
            ))
        except THREDDSCatalogError as e:
            logger.error(e)
            raise

        # Map dates to opendap urls.
        # TODO

    def get_thredds_catalog_url_for_year(self, year: int) -> str:
        home_dir = "opendap/OCO2_L2_Lite_FP.11.1r"
        return f"{self._settings.earthdata_base_url}/{home_dir}/{year}/catalog.xml"
