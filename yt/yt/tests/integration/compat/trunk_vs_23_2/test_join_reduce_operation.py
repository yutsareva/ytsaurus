from original_tests.yt.yt.tests.integration.controller.test_join_reduce_operation \
    import TestSchedulerJoinReduceCommands as BaseTestJoinReduceCommands
import pytest


@pytest.mark.opensource
class TestJoinReduceCommandsCompatNewCA(BaseTestJoinReduceCommands):
    ARTIFACT_COMPONENTS = {
        "23_2": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }


@pytest.mark.opensource
class TestJoinReduceCommandsCompatNewNodes(BaseTestJoinReduceCommands):
    ARTIFACT_COMPONENTS = {
        "23_2": ["master", "scheduler", "controller-agent"],
        "trunk": ["node", "job-proxy", "exec", "tools", "proxy", "http-proxy"],
    }
