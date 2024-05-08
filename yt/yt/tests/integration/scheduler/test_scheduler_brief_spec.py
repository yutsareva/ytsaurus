from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, get, write_table, map, reduce, map_reduce,
    join_reduce, merge,
    sort, erase)
import pytest

##################################################################


def check_attributes(op, options):
    spec_path = op.get_path() + "/@spec"
    brief_spec_path = op.get_path() + "/@brief_spec"

    if "pool" in options:
        assert get(spec_path + "/pool") == get(brief_spec_path + "/pool")
    if "reducer" in options:
        assert get(spec_path + "/reducer/command") == get(brief_spec_path + "/reducer/command")
    if "mapper" in options:
        assert get(spec_path + "/mapper/command") == get(brief_spec_path + "/mapper/command")
    if "table_path" in options:
        assert get(spec_path + "/table_path") == get(brief_spec_path + "/table_path")

    if "input_table_path" in options:
        assert get(brief_spec_path + "/input_table_paths/@count") == len(list(get(spec_path + "/input_table_paths")))
        assert get(spec_path + "/input_table_paths/0") == get(brief_spec_path + "/input_table_paths/0")

    if "output_table_path" in options:
        assert get(brief_spec_path + "/output_table_paths/@count") == len(list(get(spec_path + "/output_table_paths")))
        assert get(spec_path + "/output_table_paths/0") == get(brief_spec_path + "/output_table_paths/0")

    if "output_table_path_1" in options:
        assert get(brief_spec_path + "/output_table_paths/@count") == 1
        assert get(spec_path + "/output_table_path") == get(brief_spec_path + "/output_table_paths/0")


@pytest.mark.opensource
class TestSchedulerBriefSpec(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    @authors("acid", "babenko", "ignat")
    def test_map(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        op = map(in_="//tmp/t1", out="//tmp/t2", command="cat")

        check_attributes(op, ["mapper", "input_table_path", "output_table_path"])

    @authors("babenko", "klyachin")
    def test_sort(self):
        create("table", "//tmp/t1")

        op = sort(in_="//tmp/t1", out="//tmp/t1", sort_by="key")

        check_attributes(op, ["input_table_path", "output_table_path_1"])

    @authors("ignat", "klyachin")
    def test_reduce(self):
        create("table", "//tmp/t1")
        write_table(
            "//tmp/t1",
            [
                {"key": 9, "value": 7},
            ],
            sorted_by=["key", "value"],
        )

        create("table", "//tmp/t2")
        op = reduce(in_="//tmp/t1", out="//tmp/t2", command="cat", reduce_by="key")

        check_attributes(op, ["reducer", "input_table_path", "output_table_path"])

    @authors("klyachin")
    def test_join_reduce(self):
        create("table", "//tmp/t1")
        write_table(
            "//tmp/t1",
            [
                {"key": 9, "value": 7},
            ],
            sorted_by=["key", "value"],
        )

        create("table", "//tmp/t2")
        write_table(
            "//tmp/t2",
            [
                {"key": 9, "value": 11},
            ],
            sorted_by=["key", "value"],
        )

        create("table", "//tmp/t3")
        op = join_reduce(
            in_=["//tmp/t1", "<foreign=true>//tmp/t2"],
            out="//tmp/t3",
            command="cat",
            join_by="key",
            spec={"reducer": {"format": "dsv"}},
        )

        check_attributes(op, ["reducer", "input_table_path", "output_table_path"])

    @authors("klyachin")
    def test_map_reduce(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        op = map_reduce(
            in_="//tmp/t1",
            out="//tmp/t2",
            sort_by="a",
            mapper_command="cat",
            reduce_combiner_command="cat",
            reducer_command="cat",
        )

        check_attributes(op, ["mapper", "reducer", "input_table_path", "output_table_path"])

    @authors("klyachin")
    def test_merge(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")

        op = merge(mode="unordered", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t3")

        check_attributes(op, ["input_table_path", "output_table_path_1"])

    @authors("babenko")
    def test_erase(self):
        create("table", "//tmp/t1")

        op = erase("//tmp/t1")

        check_attributes(op, ["table_path"])
