from conftest_lib.conftest_queries import QueryTracker

from yt_env_setup import YTEnvSetup

from yt.environment.init_query_tracker_state import get_latest_version

from yt.common import YtError

from yt_commands import wait, authors, ls, get, set, assert_yt_error

import yt_error_codes
import pytest


class TestEnvironment(YTEnvSetup):
    NUM_QUERY_TRACKERS = 3

    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    def _ls_instances(self):
        try:
            return ls("//sys/query_tracker/instances", verbose=False, verbose_error=False)
        except YtError as err:
            if err.contains_code(yt_error_codes.ResolveErrorCode):
                return []
            raise

    def _check_liveness(self, instance_count):
        wait(lambda: len(self._ls_instances()) == instance_count)

    def _check_cleanliness(self):
        assert len(self._ls_instances()) == 0

    @authors("max42")
    def test_fixture(self, query_tracker):
        self._check_liveness(3)

    @authors("max42")
    def test_context_manager(self, query_tracker_environment):
        self._check_cleanliness()
        with QueryTracker(self.Env, 1):
            self._check_liveness(1)
        self._check_cleanliness()
        with QueryTracker(self.Env, 2):
            self._check_liveness(2)
        self._check_cleanliness()

    @authors("mpereskokova")
    def test_alerts(self, query_tracker):
        alerts_path = f"//sys/query_tracker/instances/{query_tracker.addresses[0]}/orchid/alerts"
        version_path = "//sys/query_tracker/@version"
        latest_version = get_latest_version()

        assert get(version_path) == latest_version
        assert len(get(alerts_path)) == 0

        set(version_path, latest_version - 1)
        wait(lambda: "query_tracker_invalid_state" in get(alerts_path))
        assert_yt_error(YtError.from_dict(get(alerts_path)["query_tracker_invalid_state"]),
                        "Min required state version is not met")
