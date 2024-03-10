import pandas as pd
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from data.loaders.influxdb_loader import InfluxDBLoader


@pytest.mark.integration
class TestInfluxDBLoader:

    @pytest.fixture(scope="module")
    def influxdb_container(self, dummy_settings) -> DockerContainer:  # Do not use `Iterator` here!
        container = DockerContainer("influxdb:2.7.4-alpine") \
            .with_env("DOCKER_INFLUXDB_INIT_MODE", "setup") \
            .with_env("DOCKER_INFLUXDB_INIT_USERNAME", "dummy-user") \
            .with_env("DOCKER_INFLUXDB_INIT_PASSWORD", "dummy-password") \
            .with_env("DOCKER_INFLUXDB_INIT_ORG", dummy_settings.influxdb_org) \
            .with_env("DOCKER_INFLUXDB_INIT_BUCKET", dummy_settings.influxdb_bucket) \
            .with_env("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", dummy_settings.influxdb_token) \
            .with_env("INFLUXD_UI_DISABLED", "true") \
            .with_bind_ports(8086, 8086)

        with container:
            wait_for_logs(container, "Listening", timeout=10)
            yield container

    def test_loader_workflow(self, influxdb_container, dummy_settings, dummy_df):
        assert isinstance(dummy_df, pd.DataFrame) and len(dummy_df) > 0  # Sanity check

        loader = InfluxDBLoader(settings=dummy_settings)

        loader.save_dataframe(df=dummy_df)

        loaded_df = loader.retrieve_dataframe()
        pd.testing.assert_frame_equal(loaded_df, dummy_df, check_like=True)
