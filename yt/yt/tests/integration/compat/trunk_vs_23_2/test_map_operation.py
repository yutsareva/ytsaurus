from original_tests.yt.yt.tests.integration.controller.test_map_operation \
    import TestSchedulerMapCommands as BaseTestMapCommands

from yt_commands import authors
import pytest

class TestMapCommandsCompatNewCA(BaseTestMapCommands):
    ARTIFACT_COMPONENTS = {
        "23_2": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }

    # TODO(levysotsky, gritukan): Drop me!
    @authors("levysotsky")
    def test_rename_with_both_columns_in_filter(self):
        pass


class TestMapCommandsCompatNewNodes(BaseTestMapCommands):
    ARTIFACT_COMPONENTS = {
        "23_2": ["master", "scheduler", "controller-agent", "exec", "tools", "node", "job-proxy"],
        "trunk": ["proxy", "http-proxy"],
    }
