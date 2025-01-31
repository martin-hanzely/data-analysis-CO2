from __future__ import annotations

import datetime
import logging
from typing import TYPE_CHECKING

# noinspection PyPep8Naming
import netCDF4 as nc
import pandas as pd

from data.extractors.base_opendap_extractor import BaseOpendapExtractor
from data.utils.opendap import THREDDSCatalogError

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


logger = logging.getLogger(__name__)


class OpendapExtractorL2LiteFP(BaseOpendapExtractor):
    """
    Extractor class for data from the NASA Earth Data GES DISC OPeNDAP server.
    """
    def extract_date_range(self, date_range: Iterable[datetime.date]) -> Iterator[tuple[datetime.date, pd.DataFrame]]:
        date_list = list(date_range)
        if len(date_list) < 1:
            yield from ()

        base_year = date_list[0].year
        opendap_urls_dict = self.get_opendap_urls_dict_for_year(base_year)

        seen_years = {base_year}
        for date in date_list:
            try:
                if date.year not in seen_years:
                    seen_years.add(date.year)
                    opendap_urls_dict |= self.get_opendap_urls_dict_for_year(date.year)

                date_str = date.strftime("%y%m%d")
                url = opendap_urls_dict.get(date_str)
                if url is None:
                    logger.warning("No OPeNDAP URL found for date %s", date_str)
                    continue

                logger.info("Extracting data from OPeNDAP URL %s", url)
                yield date, self.get_dataframe_from_opendap_url(url)
            except Exception as e:
                logger.error("Error processing date %s: %s", date, e)

    def get_thredds_catalog_url_for_year(self, year: int) -> str:
        home_dir = "opendap/OCO2_L2_Lite_FP.11.2r"
        return f"{self._settings.earthdata_base_url}/{home_dir}/{year}/catalog.xml"

    def get_dataframe_from_opendap_url(self, url: str) -> pd.DataFrame:
        with (
            self._client.get_file_from_opendap_url(
                url,
                self._settings.earthdata_username,
                self._settings.earthdata_password
            ) as _f,
            nc.Dataset(_f.name, mode="r") as ds
        ):
            df = pd.DataFrame({
                "_time": pd.to_datetime(ds["time"][:], unit="s", origin="1970-01-01", utc=True),
                "latitude": ds["latitude"][:],
                "longitude": ds["longitude"][:],
                "xco2": ds["xco2"][:],
                "xco2_quality_flag": ds["xco2_quality_flag"][:],
            })

        return self.clean_dataframe(df)

    def get_opendap_urls_dict_for_year(self, year: int) -> dict[str, str]:
        """
        Get dictionary of OPeNDAP URLs for given year mapped to date strings in format YYMMDD.
        :param year:
        :return:
        """
        try:
            catalog_url = self.get_thredds_catalog_url_for_year(year)
            catalog_xml = self._client.get_thredds_catalog_xml(catalog_url)
            opendap_urls = list(self._client.get_opendap_urls(
                catalog_xml,
                base_url=self._settings.earthdata_base_url,
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

        try:
            return {self.opendap_url_to_date_str(_u): _u for _u in opendap_urls}
        except ValueError as e:
            logger.error(e)
            raise

    @staticmethod
    def opendap_url_to_date_str(url: str) -> str:
        """
        Extract date string in format YYMMDD from OPeNDAP URL.
        :param url:
        :return:
        :raises ValueError: If the URL is not in the expected format.
        """
        # Assuming "/OCO2_L2_Lite_FP.11.1r/2024/oco2_LtCO2_240102_B11100Ar_240229181145s.nc4" -> "240102"
        try:
            filename = url.split("/")[-1]
            return filename.split("_")[2]
        except (IndexError, AttributeError) as e:
            raise ValueError(f"Invalid OPeNDAP URL {url}") from e

    @staticmethod
    def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        # 0=Good, 1=Bad.
        return df \
            .loc[df["xco2_quality_flag"] == 0] \
            .drop(columns=["xco2_quality_flag"])
