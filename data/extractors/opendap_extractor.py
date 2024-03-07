from __future__ import annotations

import datetime
import logging
import tempfile
import os
from typing import TYPE_CHECKING

# noinspection PyPep8Naming
import netCDF4 as nc
import pandas as pd
import requests

from data.extractors.base_extractor import BaseExtractor
from data.utils.thredds_catalog import (
    THREDDSCatalogError,
    get_thredds_catalog_url_for_date,
    get_thredds_catalog_xml,
    get_opendap_urls,
)

if TYPE_CHECKING:
    from data.settings import Settings


logger = logging.getLogger(__name__)


class OpendapExtractor(BaseExtractor):
    """
    Extractor class for data from the NASA Earth Data GES DISC OPeNDAP server.
    """
    _settings: Settings

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def extract_for_date(self, date: datetime.date) -> pd.DataFrame:
        try:
            catalog_url = get_thredds_catalog_url_for_date(date)
            catalog_xml = get_thredds_catalog_xml(catalog_url)
            opendap_urls = list(get_opendap_urls(
                catalog_xml,
                file_suffix=".nc4",
                variables=[
                    "RetrievalGeometry_retrieval_latitude",
                    "RetrievalGeometry_retrieval_longitude",
                    "RetrievalHeader_retrieval_time_string",
                    "RetrievalResults_xco2",
                    "PreprocessingResults_fluorescence_qual_flag_idp",
                ]
            ))
        except THREDDSCatalogError as e:
            logger.error(e)
            raise

        for url in opendap_urls:
            # Named temporary file is required here because netCDF4 cannot read from a file-like object.
            _f = tempfile.NamedTemporaryFile(delete=False)
            with requests.Session() as session:
                session.auth = (self._settings.earthdata_username, self._settings.earthdata_password)
                response = session.get(url)
                _f.write(response.content)
                _f.close()

            with nc.Dataset(_f.name, mode="r") as ds:
                retrieval_time_string = nc.chartostring(ds["RetrievalHeader_retrieval_time_string"][:])
                df = pd.DataFrame({
                    "retrieval_datetime": pd.to_datetime(retrieval_time_string, format="%Y-%m-%dT%H:%M:%S.%fZ"),
                    "retrieval_latitude": ds["RetrievalGeometry_retrieval_latitude"][:],
                    "retrieval_longitude": ds["RetrievalGeometry_retrieval_longitude"][:],
                    "xco2": ds["RetrievalResults_xco2"][:],
                    "fluorescence_qual_flag_idp": ds["PreprocessingResults_fluorescence_qual_flag_idp"][:],
                })

                # Keep only qual_flag_idp = 0
                df = df[df["fluorescence_qual_flag_idp"] == 0]

            os.unlink(_f.name)  # Delete temporary file.
