from data.conf import get_app_settings
from data.etl.etl_pipeline import ETLPipeline
from data.extractors.utils import get_opendap_extractor_class
from data.loaders.influxdb_loader import InfluxDBLoader
from data.utils.opendap import OpendapClient


def pipeline_factory(extractor_class: str) -> ETLPipeline:
    """
    Create ETL pipeline with production settings and dependencies.
    :param extractor_class:
    :return:
    """
    # Only OPeNDAP extractors are used in production.
    # noinspection PyPep8Naming
    ExtractorCls = get_opendap_extractor_class(extractor_class)

    settings = get_app_settings()
    etl_pipeline = ETLPipeline(
        extract_strategy=ExtractorCls(settings=settings, client=OpendapClient()),
        load_strategy=InfluxDBLoader(settings=settings)
    )

    return etl_pipeline
