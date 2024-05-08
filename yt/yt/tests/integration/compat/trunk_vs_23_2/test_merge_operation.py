from original_tests.yt.yt.tests.integration.controller.test_merge_operation \
    import TestSchedulerMergeCommands as BaseTestMergeCommands
import pytest

@pytest.mark.opensource
class TestMergeCommandsCompatNewCA(BaseTestMergeCommands):
    ARTIFACT_COMPONENTS = {
        "23_2": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }


@pytest.mark.opensource
class TestMergeCommandsCompatNewNodes(BaseTestMergeCommands):
    ARTIFACT_COMPONENTS = {
        "23_2": ["master", "scheduler", "controller-agent"],
        "trunk": ["node", "job-proxy", "exec", "tools", "proxy", "http-proxy"],
    }
