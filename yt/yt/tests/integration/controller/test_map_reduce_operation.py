from yt_env_setup import NODES_SERVICE, Restarter, YTEnvSetup

from yt_commands import (
    authors, print_debug, wait, release_breakpoint, wait_breakpoint, with_breakpoint, events_on_fs, create, create_tmpdir,
    ls, get, sorted_dicts,
    set, remove, exists, create_user, make_ace, start_transaction, commit_transaction, write_file, read_table,
    write_table, map, reduce, map_reduce, sort, alter_table, start_op,
    abandon_job, abort_job, get_operation,
    raises_yt_error,
    ban_node, unban_node)

from yt_type_helpers import struct_type, list_type, tuple_type, optional_type, make_schema, make_column

from yt_helpers import skip_if_no_descending, skip_if_renaming_disabled

from yt.common import YtError
from yt.environment.helpers import assert_items_equal

import pytest

from collections import defaultdict
from random import shuffle
import datetime
import os

from flaky import flaky

##################################################################


@pytest.mark.opensource
class TestSchedulerMapReduceCommands(YTEnvSetup):
    NUM_TEST_PARTITIONS = 8
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "sort_operation_options": {
                "min_uncompressed_block_size": 1
            },
            "map_reduce_operation_options": {
                "min_uncompressed_block_size": 1,
                "job_splitter": {
                    "min_job_time": 3000,
                    "min_total_data_size": 1024,
                    "update_period": 100,
                    "candidate_percentile": 0.8,
                    "max_jobs_per_split": 3,
                    "job_logging_period": 0,
                },
                "spec_template": {
                    "use_new_sorted_pool": False,
                }
            },
            "enable_partition_map_job_size_adjustment": True,
            "operation_options": {
                "spec_template": {
                    "enable_table_index_if_has_trivial_mapper": True,
                }
            },
            "job_tracker": {
                "node_disconnection_timeout": 2000,
            },
        }
    }

    TWO_INPUT_SCHEMAFUL_REDUCER_TEMPLATE = """
import os, sys, json
table_index = 0
rows_got = 0
for l in sys.stdin:
    row = json.loads(l)
    if "$attributes" in row:
        assert row["$value"] is None
        table_index = row["$attributes"]["table_index"]
        continue
    if rows_got == 0:
        s = 0
    if table_index == 0:
        s += row["struct"]["a"]
        b = row["struct"]["b"]
    else:
        assert table_index == 1
        s += row["{second_struct}"]["{second_struct_a}"]
    rows_got += 1
    if rows_got == 2:
        sys.stdout.write(json.dumps({{"a": row["a"], "struct2": {{"a2": s, "b2": b}}}}) + "\\n")
        rows_got = 0
"""

    TWO_INPUT_SCHEMAFUL_REDUCER = TWO_INPUT_SCHEMAFUL_REDUCER_TEMPLATE.format(
        second_struct="struct1",
        second_struct_a="a1",
    ).encode()
    TWO_INPUT_SCHEMAFUL_REDUCER_IDENTICAL_SCHEMAS = TWO_INPUT_SCHEMAFUL_REDUCER_TEMPLATE.format(
        second_struct="struct",
        second_struct_a="a",
    ).encode()

    DROP_TABLE_INDEX_REDUCER = b"""
import sys, json
for l in sys.stdin:
    row = json.loads(l)
    if "$attributes" in row:
        assert row["$value"] is None
        assert "table_index" in row["$attributes"]
        continue
    sys.stdout.write(json.dumps(row) + "\\n")
"""

    def skip_if_legacy_sorted_pool(self):
        if not isinstance(self, TestSchedulerMapReduceCommandsNewSortedPool):
            pytest.skip("This test requires new sorted pool")

    @pytest.mark.parametrize(
        "method",
        [
            "map_sort_reduce",
            "map_reduce",
            "map_reduce_1p",
            "reduce_combiner_dev_null",
            "force_reduce_combiners",
            "ordered_map_reduce",
            "map_reduce_with_hierarchical_partitions",
        ],
    )
    @authors("ignat")
    def test_simple(self, method):
        text = """
So, so you think you can tell Heaven from Hell,
blue skies from pain.
Can you tell a green field from a cold steel rail?
A smile from a veil?
Do you think you can tell?
And did they get you to trade your heroes for ghosts?
Hot ashes for trees?
Hot air for a cool breeze?
Cold comfort for change?
And did you exchange a walk on part in the war for a lead role in a cage?
How I wish, how I wish you were here.
We're just two lost souls swimming in a fish bowl, year after year,
Running over the same old ground.
What have you found? The same old fears.
Wish you were here.
"""

        # remove punctuation from text
        stop_symbols = ",.?"
        for s in stop_symbols:
            text = text.replace(s, " ")

        mapper = b"""
import sys

for line in sys.stdin:
    for word in line.lstrip("line=").split():
        print "word=%s\\tcount=1" % word
"""
        reducer = b"""
import sys

from itertools import groupby

def read_table():
    for line in sys.stdin:
        row = {}
        fields = line.strip().split("\t")
        for field in fields:
            key, value = field.split("=", 1)
            row[key] = value
        yield row

for key, rows in groupby(read_table(), lambda row: row["word"]):
    count = sum(int(row["count"]) for row in rows)
    print "word=%s\\tcount=%s" % (key, count)
"""

        tx = start_transaction(timeout=60000)

        create("table", "//tmp/t_in", tx=tx)
        create("table", "//tmp/t_map_out", tx=tx)
        create("table", "//tmp/t_reduce_in", tx=tx)
        create("table", "//tmp/t_out", tx=tx)

        for line in text.split("\n"):
            write_table("<append=true>//tmp/t_in", {"line": line}, tx=tx)

        create("file", "//tmp/yt_streaming.py")
        create("file", "//tmp/mapper.py")
        create("file", "//tmp/reducer.py")

        write_file("//tmp/mapper.py", mapper, tx=tx)
        write_file("//tmp/reducer.py", reducer, tx=tx)

        if method == "map_sort_reduce":
            map(
                in_="//tmp/t_in",
                out="//tmp/t_map_out",
                command="python mapper.py",
                file=["//tmp/mapper.py", "//tmp/yt_streaming.py"],
                spec={"mapper": {"format": "dsv"}},
                tx=tx,
            )

            sort(in_="//tmp/t_map_out", out="//tmp/t_reduce_in", sort_by="word", tx=tx)

            reduce(
                in_="//tmp/t_reduce_in",
                out="//tmp/t_out",
                reduce_by="word",
                command="python reducer.py",
                file=["//tmp/reducer.py", "//tmp/yt_streaming.py"],
                spec={"reducer": {"format": "dsv"}},
                tx=tx,
            )
        elif method == "map_reduce":
            map_reduce(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                sort_by="word",
                mapper_command="python mapper.py",
                mapper_file=["//tmp/mapper.py", "//tmp/yt_streaming.py"],
                reduce_combiner_command="python reducer.py",
                reduce_combiner_file=["//tmp/reducer.py", "//tmp/yt_streaming.py"],
                reducer_command="python reducer.py",
                reducer_file=["//tmp/reducer.py", "//tmp/yt_streaming.py"],
                spec={
                    "partition_count": 2,
                    "map_job_count": 2,
                    "mapper": {"format": "dsv"},
                    "reduce_combiner": {"format": "dsv"},
                    "reducer": {"format": "dsv"},
                    "data_size_per_sort_job": 10,
                },
                tx=tx,
            )
        elif method == "map_reduce_1p":
            map_reduce(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                sort_by="word",
                mapper_command="python mapper.py",
                mapper_file=["//tmp/mapper.py", "//tmp/yt_streaming.py"],
                reducer_command="python reducer.py",
                reducer_file=["//tmp/reducer.py", "//tmp/yt_streaming.py"],
                spec={
                    "partition_count": 1,
                    "mapper": {"format": "dsv"},
                    "reducer": {"format": "dsv"},
                },
                tx=tx,
            )
        elif method == "reduce_combiner_dev_null":
            map_reduce(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                sort_by="word",
                mapper_command="python mapper.py",
                mapper_file=["//tmp/mapper.py", "//tmp/yt_streaming.py"],
                reduce_combiner_command="cat >/dev/null",
                reducer_command="python reducer.py",
                reducer_file=["//tmp/reducer.py", "//tmp/yt_streaming.py"],
                spec={
                    "partition_count": 2,
                    "map_job_count": 2,
                    "mapper": {"format": "dsv"},
                    "reduce_combiner": {"format": "dsv"},
                    "reducer": {"format": "dsv"},
                    "data_size_per_sort_job": 10,
                },
                tx=tx,
            )
        elif method == "force_reduce_combiners":
            map_reduce(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                sort_by="word",
                mapper_command="python mapper.py",
                mapper_file=["//tmp/mapper.py", "//tmp/yt_streaming.py"],
                reduce_combiner_command="python reducer.py",
                reduce_combiner_file=["//tmp/reducer.py", "//tmp/yt_streaming.py"],
                reducer_command="cat",
                spec={
                    "partition_count": 2,
                    "map_job_count": 2,
                    "mapper": {"format": "dsv"},
                    "reduce_combiner": {"format": "dsv"},
                    "reducer": {"format": "dsv"},
                    "force_reduce_combiners": True,
                },
                tx=tx,
            )
        elif method == "ordered_map_reduce":
            map_reduce(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                sort_by="word",
                mapper_command="python mapper.py",
                mapper_file=["//tmp/mapper.py", "//tmp/yt_streaming.py"],
                reduce_combiner_command="python reducer.py",
                reduce_combiner_file=["//tmp/reducer.py", "//tmp/yt_streaming.py"],
                reducer_command="python reducer.py",
                reducer_file=["//tmp/reducer.py", "//tmp/yt_streaming.py"],
                spec={
                    "partition_count": 2,
                    "map_job_count": 2,
                    "mapper": {"format": "dsv"},
                    "reduce_combiner": {"format": "dsv"},
                    "reducer": {"format": "dsv"},
                    "data_size_per_sort_job": 10,
                    "ordered": True,
                },
                tx=tx,
            )
        elif method == "map_reduce_with_hierarchical_partitions":
            map_reduce(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                sort_by="word",
                mapper_command="python mapper.py",
                mapper_file=["//tmp/mapper.py", "//tmp/yt_streaming.py"],
                reduce_combiner_command="python reducer.py",
                reduce_combiner_file=["//tmp/reducer.py", "//tmp/yt_streaming.py"],
                reducer_command="python reducer.py",
                reducer_file=["//tmp/reducer.py", "//tmp/yt_streaming.py"],
                spec={
                    "partition_count": 7,
                    "max_partition_factor": 2,
                    "map_job_count": 2,
                    "mapper": {"format": "dsv"},
                    "reduce_combiner": {"format": "dsv"},
                    "reducer": {"format": "dsv"},
                    "data_size_per_sort_job": 10,
                },
                tx=tx,
            )
        else:
            assert False

        commit_transaction(tx)

        # count the desired output
        expected = defaultdict(int)
        for word in text.split():
            expected[word] += 1

        output = []
        if method != "reduce_combiner_dev_null":
            for word, count in expected.items():
                output.append({"word": word, "count": str(count)})
            assert_items_equal(read_table("//tmp/t_out"), output)
        else:
            assert_items_equal(read_table("//tmp/t_out"), output)

    @authors("ignat")
    @pytest.mark.parametrize("ordered", [False, True])
    def test_many_output_tables(self, ordered):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")
        write_table("//tmp/t_in", {"line": "some_data"})
        map_reduce(
            in_="//tmp/t_in",
            out=["//tmp/t_out1", "//tmp/t_out2"],
            sort_by="line",
            reducer_command="cat",
            spec={"reducer": {"format": "dsv"}, "ordered": ordered},
        )

    @authors("coteeq")
    @pytest.mark.parametrize("op_type,mapper_tables", [
        ("map", None),
        ("reduce", None),
        ("map_reduce", 0),
        ("map_reduce", 1),
        ("map_reduce", 2),
    ])
    def test_duplicate_output_tables(self, op_type, mapper_tables):
        if self.Env.get_component_version("ytserver-controller-agent").abi <= (23, 2):
            pytest.skip()

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        data = [
            {"line": "some_data"},
            {"line": "other_data"},
        ]
        write_table("//tmp/t_in", data)

        kwargs = dict(
            in_="//tmp/t_in",
            out=["//tmp/t_out", "//tmp/t_out", "//tmp/t_out"],
            spec={
                "mapper": {"format": "json"},
                "reducer": {"format": "json"},
            },
        )

        if mapper_tables:
            kwargs["spec"]["mapper_output_table_count"] = mapper_tables
        if op_type in ("map", "reduce"):
            kwargs["command"] = "cat"
        if op_type == "map_reduce":
            kwargs["mapper_command"] = "cat"
            kwargs["reducer_command"] = "cat"
            kwargs["sort_by"] = "line"
            kwargs["reduce_by"] = "line"

        with raises_yt_error("Duplicate entries in output_table_paths"):
            start_op(op_type, **kwargs)

    @authors("psushin")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_reduce_with_sort(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"x": 1, "y": 2}, {"x": 1, "y": 1}, {"x": 1, "y": 3}])

        write_table(
            "<append=true>//tmp/t_in",
            [{"x": 2, "y": 3}, {"x": 2, "y": 2}, {"x": 2, "y": 4}],
        )

        reducer = b"""
import sys
for l in sys.stdin:
  l = l.strip('\\n')
  pairs = l.split('\\t')
  pairs = [a.split("=") for a in pairs]
  d = dict([(a[0], int(a[1])) for a in pairs])
  x = d['x']
  y = d['y']
  print l
print "x={0}\ty={1}".format(x, y)
"""

        create("file", "//tmp/reducer.py")
        write_file("//tmp/reducer.py", reducer)

        sorted_by = "[{{name=x; sort_order={0}}};{{name=y; sort_order={0}}}]".format(sort_order)
        sort_by = [{"name": name, "sort_order": sort_order} for name in ["x", "y"]]
        map_reduce(
            in_="//tmp/t_in",
            out="<sorted_by={}>//tmp/t_out".format(sorted_by),
            reduce_by="x",
            sort_by=sort_by,
            reducer_file=["//tmp/reducer.py"],
            reducer_command="python reducer.py",
            spec={"partition_count": 2, "reducer": {"format": "dsv"}},
        )

        if sort_order == "ascending":
            expected = [
                {"x": "1", "y": "1"},
                {"x": "1", "y": "2"},
                {"x": "1", "y": "3"},
                {"x": "1", "y": "3"},
                {"x": "2", "y": "2"},
                {"x": "2", "y": "3"},
                {"x": "2", "y": "4"},
                {"x": "2", "y": "4"},
            ]
        else:
            expected = [
                {"x": "2", "y": "4"},
                {"x": "2", "y": "3"},
                {"x": "2", "y": "2"},
                {"x": "2", "y": "2"},
                {"x": "1", "y": "3"},
                {"x": "1", "y": "2"},
                {"x": "1", "y": "1"},
                {"x": "1", "y": "1"},
            ]

        assert read_table("//tmp/t_out") == expected
        assert get("//tmp/t_out/@sorted")

    @authors("babenko")
    def test_row_count_limit(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"x": 1, "y": 2}])
        write_table("<append=true>//tmp/t_in", [{"x": 2, "y": 3}])

        map_reduce(
            in_="//tmp/t_in",
            out="<row_count_limit=1>//tmp/t_out",
            reduce_by="x",
            sort_by="x",
            reducer_command="cat",
            spec={
                "partition_count": 2,
                "reducer": {"format": "dsv"},
                "resource_limits": {"user_slots": 1},
            },
        )

        assert len(read_table("//tmp/t_out")) == 1

    @authors("gritukan")
    def test_row_count_limit_sorted_reduce(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"x": 1, "y": 2, "z": "A" * 100000}])
        write_table("<append=true>//tmp/t_in", [{"x": 2, "y": 3, "z": "A" * 100000}])

        map_reduce(
            in_="//tmp/t_in",
            out="<row_count_limit=2>//tmp/t_out",
            reduce_by="x",
            sort_by="x",
            reducer_command="cat",
            spec={
                "partition_count": 1,
                "data_size_per_map_job": 1,
                "data_size_per_sort_job": 1,
                "reducer": {"format": "dsv"},
            },
        )

        assert len(read_table("//tmp/t_out", verbose=False)) == 2

    @authors("gritukan")
    def test_force_complete_sorted_reduce(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        for i in range(10):
            write_table("<append=true>//tmp/t_in", [{"x": i, "y": 2, "z": "A" * 100000}])

        op = map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            reduce_by="x",
            sort_by="x",
            reducer_command=with_breakpoint("BREAKPOINT; cat"),
            spec={
                "partition_count": 2,
                "data_size_per_map_job": 1,
                "data_size_per_sort_job": 1,
                "reducer": {"format": "dsv"},
            },
            track=False,
        )

        some_job = wait_breakpoint()[0]
        release_breakpoint(job_id=some_job)

        def get_completed_reduce_job_count():
            path = op.get_path() + "/@progress/data_flow_graph/vertices/sorted_reduce/job_counter"
            if not exists(path):
                return 0
            return get(path)["completed"]["total"]

        wait(lambda: get_completed_reduce_job_count() > 0)
        op.complete()

        assert len(read_table("//tmp/t_out", verbose=False)) > 0

    @authors("levysotsky")
    def test_intermediate_live_preview(self):
        create_user("u")
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"foo": "bar"})
        create("table", "//tmp/t2")

        create_user("admin")
        set("//sys/@config/cypress_manager/portal_synchronization_period", 500)
        set(
            "//sys/operations&/@acl/end",
            make_ace("allow", "admin", ["read", "write", "manage"]),
        )
        wait(lambda: get("//sys/operations&/@acl") == get("//sys/operations/@acl"))

        def is_admin_in_base_acl():
            op = map(
                command="cat",
                in_="//tmp/t1",
                out="//tmp/t2",
            )
            return any(ace["subjects"] == ["admin"] for ace in get_operation(op.id)["runtime_parameters"]["acl"])

        wait(is_admin_in_base_acl)

        try:

            op = map_reduce(
                track=False,
                mapper_command="cat",
                reducer_command=with_breakpoint("cat; BREAKPOINT"),
                in_="//tmp/t1",
                out="//tmp/t2",
                sort_by=["foo"],
                spec={
                    "acl": [make_ace("allow", "u", ["read", "manage"])],
                },
            )

            wait(lambda: op.get_job_count("completed") == 1)

            operation_path = op.get_path()
            scheduler_transaction_id = get(operation_path + "/@async_scheduler_transaction_id")
            assert exists(operation_path + "/intermediate", tx=scheduler_transaction_id)

            intermediate_acl = get(operation_path + "/intermediate/@acl", tx=scheduler_transaction_id)
            assert sorted_dicts(intermediate_acl) == sorted_dicts(
                [
                    # "authenticated_user" of operation.
                    make_ace("allow", "root", "read"),
                    # User from operation ACL.
                    make_ace("allow", "u", "read"),
                    # User from operation base ACL (from "//sys/operations/@acl").
                    make_ace("allow", "admin", "read"),
                ]
            )

            release_breakpoint()
            op.track()
            assert read_table("//tmp/t2") == [{"foo": "bar"}]
        finally:
            remove("//sys/operations&/@acl/-1")
            wait(lambda: get("//sys/operations/@acl") == get("//sys/operations&/@acl"))

    @authors("levysotsky")
    def test_intermediate_new_live_preview(self):
        partition_map_vertex = "partition_map(0)"

        create_user("admin")
        set("//sys/@config/cypress_manager/portal_synchronization_period", 500)
        set(
            "//sys/operations&/@acl/end",
            make_ace("allow", "admin", ["read", "write", "manage"]),
        )
        wait(lambda: get("//sys/operations&/@acl") == get("//sys/operations/@acl"))
        try:
            create_user("u")
            create("table", "//tmp/t1")
            write_table("//tmp/t1", {"foo": "bar"})
            create("table", "//tmp/t2")

            op = map_reduce(
                track=False,
                mapper_command="cat",
                reducer_command=with_breakpoint("cat; BREAKPOINT"),
                in_="//tmp/t1",
                out="//tmp/t2",
                sort_by=["foo"],
                spec={
                    "acl": [make_ace("allow", "u", ["read"])],
                },
            )

            wait(lambda: op.get_job_count("completed") == 1)

            operation_path = op.get_path()
            get(operation_path + "/controller_orchid/data_flow_graph/vertices")
            intermediate_live_data = read_table(
                operation_path
                + "/controller_orchid/data_flow_graph/vertices/{}/live_previews/0".format(partition_map_vertex)
            )

            release_breakpoint()
            op.track()
            assert intermediate_live_data == [{"foo": "bar"}]
            assert read_table("//tmp/t2") == [{"foo": "bar"}]
        finally:
            remove("//sys/operations&/@acl/-1")

    @authors("ignat")
    def test_intermediate_compression_codec(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"foo": "bar"})
        create("table", "//tmp/t2")

        op = map_reduce(
            track=False,
            mapper_command="cat",
            reducer_command="sleep 5; cat",
            in_="//tmp/t1",
            out="//tmp/t2",
            sort_by=["foo"],
            spec={"intermediate_compression_codec": "brotli_3"},
        )
        operation_path = op.get_path()
        wait(lambda: exists(operation_path + "/@async_scheduler_transaction_id"))
        async_transaction_id = get(operation_path + "/@async_scheduler_transaction_id")
        wait(lambda: exists(operation_path + "/intermediate", tx=async_transaction_id))
        assert "brotli_3" == get(operation_path + "/intermediate/@compression_codec", tx=async_transaction_id)
        op.abort()

    @authors("savrus")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_query_simple(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        map_reduce(
            in_="//tmp/t1",
            out="//tmp/t2",
            mapper_command="cat",
            reducer_command="cat",
            sort_by=[{"name": "a", "sort_order": sort_order}],
            spec={
                "input_query": "a",
                "input_schema": [{"name": "a", "type": "string"}],
            },
        )

        assert read_table("//tmp/t2") == [{"a": "b"}]

    @authors("babenko", "dakovalkov")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_rename_columns_simple(self, optimize_for, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create(
            "table",
            "//tmp/tin",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "b", "type": "int64"},
                ],
                "optimize_for": optimize_for,
            },
        )
        create("table", "//tmp/tout")
        write_table("//tmp/tin", [{"a": 42, "b": 25}])

        map_reduce(
            in_="<rename_columns={a=b;b=a}>//tmp/tin",
            out="//tmp/tout",
            mapper_command="cat",
            reducer_command="cat",
            sort_by=[{"name": "a", "sort_order": sort_order}],
        )

        assert read_table("//tmp/tout") == [{"b": 42, "a": 25}]

    @authors("levysotsky")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_rename_columns_alter_table(self, optimize_for, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
        skip_if_renaming_disabled(self.Env)

        input_table = "//tmp/tin"
        output_table = "//tmp/tout"

        schema1 = make_schema([
            make_column("a", "int64"),
            make_column("b", "string"),
            make_column("c", "bool"),
        ])
        create(
            "table",
            input_table,
            attributes={
                "schema": schema1,
                "optimize_for": optimize_for,
            },
        )
        create("table", output_table)
        write_table(input_table, [{"a": 42, "b": "42", "c": False}])

        schema2 = make_schema([
            make_column("b", "string"),
            make_column("c_new", "bool", stable_name="c"),
            make_column("a_new", "int64", stable_name="a"),
        ])
        alter_table(input_table, schema=schema2)

        write_table("<append=%true>" + input_table, [{"a_new": 43, "b": "43", "c_new": False}])

        map_reduce(
            in_=input_table,
            out=output_table,
            mapper_command="cat",
            reducer_command="cat",
            sort_by=[{"name": "a_new", "sort_order": sort_order}],
        )

        assert sorted(read_table(output_table), key=lambda r: r["a_new"]) == [
            {"a_new": 42, "b": "42", "c_new": False},
            {"a_new": 43, "b": "43", "c_new": False},
        ]

    def _find_intermediate_chunks(self):
        # Figure out the intermediate chunk
        chunks = ls("//sys/chunks", attributes=["requisition"])
        return [
            str(c)
            for c in chunks
            if c.attributes["requisition"][0]["account"] == "intermediate"
        ]

    def _ban_nodes_with_intermediate_chunks(self):
        intermediate_chunk_ids = self._find_intermediate_chunks()
        assert len(intermediate_chunk_ids) == 1
        intermediate_chunk_id = intermediate_chunk_ids[0]

        replicas = get("#{}/@stored_replicas".format(intermediate_chunk_id))
        assert len(replicas) == 1
        node_id = replicas[0]

        ban_node(node_id, "node with intermediate chunk")
        wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/{}/master_state".format(node_id)) == "offline")

        controller_agent_addresses = ls("//sys/controller_agents/instances")
        for controller_agent_address in controller_agent_addresses:
            wait(lambda: not exists("//sys/controller_agents/instances/{}/orchid/controller_agent/job_tracker/nodes/{}".format(
                controller_agent_address, node_id)))
        return [node_id]

    def _abort_single_job_if_running_after_node_ban(self, op, job_id):
        jobs = op.get_running_jobs().keys()
        if len(jobs) > 0:
            assert len(jobs) == 1
            running_job = list(jobs)[0]
            if running_job == job_id:
                abort_job(running_job)

    @authors("psushin")
    @pytest.mark.parametrize("ordered", [False, True])
    def test_lost_jobs(self, ordered):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"x": 1, "y": 2}, {"x": 2, "y": 3}] * 5)

        op = map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            reduce_by="x",
            sort_by="x",
            reducer_command=with_breakpoint("cat;BREAKPOINT"),
            spec={
                "sort_locality_timeout": 0,
                "sort_assignment_timeout": 0,
                "enable_partitioned_data_balancing": False,
                "intermediate_data_replication_factor": 1,
                "sort_job_io": {"table_reader": {"retry_count": 1, "pass_count": 1}},
                "ordered": ordered,
            },
            track=False,
        )

        first_reduce_job = wait_breakpoint()[0]

        self._ban_nodes_with_intermediate_chunks()

        # We abort reducer and restarted job will fail
        # due to unavailable intermediate chunk.
        # This will lead to a lost map job.
        # It can happen that running job was on banned node,
        # so we must check that we are aborting the right job.
        self._abort_single_job_if_running_after_node_ban(op, first_reduce_job)

        release_breakpoint()
        op.track()

        assert get(op.get_path() + "/@progress/partition_jobs/lost") == 1

    @authors("psushin")
    @pytest.mark.parametrize("ordered", [False, True])
    def test_unavailable_intermediate_chunks(self, ordered):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"x": 1, "y": 2}, {"x": 2, "y": 3}] * 5)

        reducer_cmd = " ; ".join(
            [
                "cat",
                events_on_fs().notify_event_cmd("reducer_started"),
                events_on_fs().wait_event_cmd("continue_reducer"),
            ]
        )

        op = map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            reduce_by="x",
            sort_by="x",
            reducer_command=reducer_cmd,
            spec={
                "enable_intermediate_output_recalculation": False,
                "sort_assignment_timeout": 0,
                "sort_locality_timeout": 0,
                "enable_partitioned_data_balancing": False,
                "intermediate_data_replication_factor": 1,
                "sort_job_io": {"table_reader": {"retry_count": 1, "pass_count": 1}},
                "partition_count": 2,
                "resource_limits": {"user_slots": 1},
                "ordered": ordered,
            },
            track=False,
        )

        # We wait for the first reducer to start (the second one is pending due to resource_limits).
        events_on_fs().wait_event("reducer_started", timeout=datetime.timedelta(1000))

        banned_nodes = self._ban_nodes_with_intermediate_chunks()

        # The first reducer will probably complete successfully, but the second one
        # must fail due to unavailable intermediate chunk.
        # This will lead to a lost map job.
        events_on_fs().notify_event("continue_reducer")

        def get_unavailable_chunk_count():
            return get(op.get_path() + "/@progress/estimated_input_statistics/unavailable_chunk_count")

        # Wait till scheduler discovers that chunk is unavailable.
        wait(lambda: get_unavailable_chunk_count() > 0)

        # Make chunk available again.
        for node in banned_nodes:
            unban_node(node)

        wait(lambda: get_unavailable_chunk_count() == 0)

        op.track()

        assert get(op.get_path() + "/@progress/partition_reduce_jobs/aborted/total") > 0
        assert get(op.get_path() + "/@progress/partition_jobs/lost") == 0

    @authors("max42")
    @pytest.mark.parametrize("ordered", [False, True])
    def test_progress_counter(self, ordered):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"x": 1, "y": 2}])

        reducer_cmd = " ; ".join(
            [
                "cat",
                events_on_fs().notify_event_cmd("reducer_started"),
                events_on_fs().wait_event_cmd("continue_reducer"),
            ]
        )

        op = map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            reduce_by="x",
            sort_by="x",
            reducer_command=reducer_cmd,
            spec={"partition_count": 1, "ordered": ordered},
            track=False,
        )

        events_on_fs().wait_event("reducer_started", timeout=datetime.timedelta(1000))

        job_ids = list(op.get_running_jobs())
        assert len(job_ids) == 1
        job_id = job_ids[0]

        abort_job(job_id)

        events_on_fs().notify_event("continue_reducer")

        op.track()

        partition_reduce_counter = get(
            op.get_path() + "/@progress/data_flow_graph/vertices/partition_reduce/job_counter"
        )

        assert partition_reduce_counter["aborted"]["total"] == 1
        assert partition_reduce_counter["pending"] == 0

    @authors("savrus")
    def test_query_reader_projection(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b", "c": "d"})

        map_reduce(
            in_="//tmp/t1",
            out="//tmp/t2",
            mapper_command="cat",
            reducer_command="cat",
            sort_by=["a"],
            spec={
                "input_query": "a",
                "input_schema": [{"name": "a", "type": "string"}],
            },
        )

        assert read_table("//tmp/t2") == [{"a": "b"}]

    @authors("savrus")
    def test_query_with_condition(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": i} for i in range(2)])

        map_reduce(
            in_="//tmp/t1",
            out="//tmp/t2",
            mapper_command="cat",
            reducer_command="cat",
            sort_by=["a"],
            spec={
                "input_query": "a where a > 0",
                "input_schema": [{"name": "a", "type": "int64"}],
            },
        )

        assert read_table("//tmp/t2") == [{"a": 1}]

    @authors("savrus", "psushin")
    def test_query_asterisk(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        rows = [{"a": 1, "b": 2, "c": 3}, {"b": 5, "c": 6}, {"a": 7, "c": 8}]
        write_table("//tmp/t1", rows)

        schema = [
            {"name": "z", "type": "int64"},
            {"name": "a", "type": "int64"},
            {"name": "y", "type": "int64"},
            {"name": "b", "type": "int64"},
            {"name": "x", "type": "int64"},
            {"name": "c", "type": "int64"},
            {"name": "u", "type": "int64"},
        ]

        for row in rows:
            for column in schema:
                if column["name"] not in row.keys():
                    row[column["name"]] = None

        map_reduce(
            in_="//tmp/t1",
            out="//tmp/t2",
            mapper_command="cat",
            reducer_command="cat",
            sort_by=["a"],
            spec={"input_query": "* where a > 0 or b > 0", "input_schema": schema},
        )

        assert_items_equal(read_table("//tmp/t2"), rows)

    @authors("babenko", "ignat", "klyachin")
    def test_bad_control_attributes(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"foo": "bar"})
        create("table", "//tmp/t2")

        with pytest.raises(YtError):
            map_reduce(
                mapper_command="cat",
                reducer_command="cat",
                in_="//tmp/t1",
                out="//tmp/t2",
                sort_by=["foo"],
                spec={"reduce_job_io": {"control_attributes": {"enable_row_index": "true"}}},
            )

    @authors("savrus")
    def test_schema_validation(self):
        create("table", "//tmp/input")
        create(
            "table",
            "//tmp/output",
            attributes={
                "schema": [
                    {"name": "key", "type": "int64"},
                    {"name": "value", "type": "string"},
                ]
            },
        )

        for i in range(10):
            write_table("<append=true; sorted_by=[key]>//tmp/input", {"key": i, "value": "foo"})

        map_reduce(
            in_="//tmp/input",
            out="//tmp/output",
            sort_by="key",
            mapper_command="cat",
            reducer_command="cat",
        )

        assert get("//tmp/output/@schema_mode") == "strong"
        assert get("//tmp/output/@schema/@strict")
        assert_items_equal(read_table("//tmp/output"), [{"key": i, "value": "foo"} for i in range(10)])

        write_table("<sorted_by=[key]>//tmp/input", {"key": "1", "value": "foo"})

        with pytest.raises(YtError):
            map_reduce(
                in_="//tmp/input",
                out="//tmp/output",
                sort_by="key",
                mapper_command="cat",
                reducer_command="cat",
            )

    @authors("savrus")
    def test_computed_columns(self):
        create("table", "//tmp/t1")
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "k1", "type": "int64", "expression": "k2 * 2"},
                    {"name": "k2", "type": "int64"},
                ]
            },
        )

        write_table("//tmp/t1", [{"k2": i} for i in range(2)])

        map_reduce(
            in_="//tmp/t1",
            out="//tmp/t2",
            sort_by="k2",
            mapper_command="cat",
            reducer_command="cat",
        )

        assert get("//tmp/t2/@schema_mode") == "strong"
        assert read_table("//tmp/t2") == [{"k1": i * 2, "k2": i} for i in range(2)]

    @authors("klyachin")
    @pytest.mark.skipif("True", reason="YT-8228")
    def test_map_reduce_job_size_adjuster_boost(self):
        create("table", "//tmp/t_input")
        # original_data should have at least 1Mb of data
        original_data = [{"index": "%05d" % i, "foo": "a" * 35000} for i in range(31)]
        for row in original_data:
            write_table("<append=true>//tmp/t_input", row, verbose=False)

        create("table", "//tmp/t_output")

        map_reduce(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            sort_by="lines",
            mapper_command="echo lines=`wc -l`",
            reducer_command="cat",
            spec={
                "mapper": {"format": "dsv"},
                "map_job_io": {"table_writer": {"block_size": 1024}},
                "resource_limits": {"user_slots": 1},
            },
        )

        expected = [{"lines": str(2 ** i)} for i in range(5)]
        actual = read_table("//tmp/t_output")
        assert_items_equal(actual, expected)

    @authors("max42")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending", None])
    @pytest.mark.parametrize("ordered", [False, True])
    def test_map_output_table(self, sort_order, ordered):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        create(
            "table",
            "//tmp/t_out_map",
            attributes={
                "schema": [
                    {
                        "name": "bypass_key",
                        "type": "int64",
                        "sort_order": sort_order,
                    }
                ]
            },
        )

        write_table("<append=%true>//tmp/t_in", [{"a": i} for i in range(10)])

        if sort_order == "descending":
            sort_by = [{"name": "shuffle_key", "sort_order": "descending"}]
        else:
            sort_by = [{"name": "shuffle_key", "sort_order": "ascending"}]
        map_reduce(
            in_="//tmp/t_in",
            out=["//tmp/t_out_map", "//tmp/t_out"],
            mapper_command="echo \"{bypass_key=$YT_JOB_INDEX}\" 1>&4; echo '{shuffle_key=23}'",
            reducer_command="cat",
            reduce_by=["shuffle_key"],
            sort_by=sort_by,
            spec={
                "mapper_output_table_count": 1,
                "max_failed_job_count": 1,
                "data_size_per_map_job": 1,
                "ordered": ordered,
            },
        )
        assert read_table("//tmp/t_out") == [{"shuffle_key": 23}] * 10
        assert len(read_table("//tmp/t_out_map")) == 10

    @authors("max42")
    @flaky(max_runs=3)
    def test_data_balancing(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        job_count = 20
        node_count = get("//sys/cluster_nodes/@count")
        write_table("//tmp/t1", [{"a": "x" * 10 ** 6} for i in range(job_count)])
        map_reduce(
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "min_locality_input_data_weight": 1,
                "enable_partitioned_data_balancing": True,
                "data_size_per_map_job": 1,
                "mapper": {"format": "dsv"},
                "reducer": {"format": "dsv"},
            },
            sort_by=["cwd"],
            mapper_command="echo cwd=`pwd`",
            reducer_command="cat",
        )

        cnt = {}
        for row in read_table("//tmp/t2"):
            cnt[row["cwd"]] = cnt.get(row["cwd"], 0) + 1
        values = cnt.values()
        print_debug(values)
        assert max(values) <= 2 * job_count // node_count

    @authors("dakovalkov")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_ordered_map_reduce(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        for i in range(50):
            write_table("<append=%true>//tmp/t_in", [{"key": i}])
        map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            mapper_command="cat",
            reducer_command="cat",
            sort_by=[{"name": "key", "sort_order": sort_order}],
            map_job_count=1,
            ordered=True,
        )

        expected = read_table("//tmp/t_in")
        if sort_order == "descending":
            expected = expected[::-1]
        assert read_table("//tmp/t_out") == expected

    @authors("babenko")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_commandless_user_job_spec(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        for i in range(50):
            write_table("<append=%true>//tmp/t_in", [{"key": i}])
        map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            reducer_command="cat",
            sort_by=[{"name": "key", "sort_order": sort_order}],
            spec={"mapper": {"cpu_limit": 1}, "reduce_combiner": {"cpu_limit": 1}},
        )

        assert_items_equal(read_table("//tmp/t_in"), read_table("//tmp/t_out"))

    @authors("max42")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_sampling(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)

        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "key", "type": "string"},
                    {"name": "value", "type": "string"},
                ]
            },
        )
        create("table", "//tmp/t2")  # This table is used for job counting.
        create("table", "//tmp/t3")  # This table will contain the cat'ed input.
        write_table(
            "//tmp/t1",
            [{"key": ("%02d" % (i // 100)), "value": "x" * 10 ** 2} for i in range(10000)],
            table_writer={"block_size": 1024},
        )

        sort_by = [{"name": "key", "sort_order": sort_order}]

        map_reduce(
            in_="//tmp/t1",
            out=["//tmp/t2", "//tmp/t3"],
            mapper_command="cat; echo '{a=1}' >&4",
            reducer_command="cat",
            sort_by=sort_by,
            spec={
                "sampling": {"sampling_rate": 0.5, "io_block_size": 10 ** 5},
                "mapper_output_table_count": 1,
            },
        )
        assert get("//tmp/t2/@row_count") == 1
        assert 0.25 * 10000 <= get("//tmp/t3/@row_count") <= 0.75 * 10000

        map_reduce(
            in_="//tmp/t1",
            out=["//tmp/t2", "//tmp/t3"],
            mapper_command="cat; echo '{a=1}' >&4",
            reducer_command="cat",
            sort_by=sort_by,
            spec={
                "sampling": {"sampling_rate": 0.5, "io_block_size": 10 ** 5},
                "map_job_count": 10,
                "mapper_output_table_count": 1,
            },
        )
        assert get("//tmp/t2/@row_count") > 1
        assert 0.25 * 10000 <= get("//tmp/t3/@row_count") <= 0.75 * 10000

        map_reduce(
            in_="//tmp/t1",
            out=["//tmp/t2", "//tmp/t3"],
            mapper_command="cat; echo '{a=1}' >&4",
            reducer_command="cat",
            sort_by=sort_by,
            spec={
                "sampling": {"sampling_rate": 1, "io_block_size": 10 ** 5},
                "map_job_count": 10,
                "mapper_output_table_count": 1,
            },
        )
        assert get("//tmp/t2/@row_count") > 1
        assert get("//tmp/t3/@row_count") == 10000

        map_reduce(
            in_="//tmp/t1",
            out=["//tmp/t2", "//tmp/t3"],
            mapper_command="cat; echo '{a=1}' >&4",
            reducer_command="cat",
            sort_by=sort_by,
            spec={
                "sampling": {"sampling_rate": 0.5, "io_block_size": 10 ** 5},
                "partition_count": 7,
                "max_partition_factor": 2,
                "map_job_count": 10,
                "mapper_output_table_count": 1,
            },
        )
        assert get("//tmp/t2/@row_count") > 1
        assert 0.25 * 10000 <= get("//tmp/t3/@row_count") <= 0.75 * 10000

    @authors("gritukan")
    def test_pivot_keys(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")

        rows = [{"key": "%02d" % key} for key in range(50)]
        shuffle(rows)
        write_table("//tmp/t1", rows)

        map_reduce(
            in_="//tmp/t1",
            out="//tmp/t2",
            mapper_command="cat",
            reducer_command="cat",
            sort_by=["key"],
            spec={"pivot_keys": [["01"], ["43"]]},
        )

        assert_items_equal(read_table("//tmp/t2"), sorted_dicts(rows))
        chunk_ids = get("//tmp/t2/@chunk_ids")
        assert sorted([get("#" + chunk_id + "/@row_count") for chunk_id in chunk_ids]) == [1, 7, 42]

        map_reduce(
            in_="//tmp/t1",
            out="//tmp/t3",
            reducer_command="cat",
            sort_by=["key"],
            spec={"pivot_keys": [["01"], ["43"]]},
        )

        assert_items_equal(read_table("//tmp/t3"), sorted_dicts(rows))
        chunk_ids = get("//tmp/t3/@chunk_ids")
        assert sorted([get("#" + chunk_id + "/@row_count") for chunk_id in chunk_ids]) == [1, 7, 42]

    @authors("gritukan")
    def test_empty_pivot_key(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")

        rows = [{"key": "%02d" % key} for key in range(50)]
        shuffle(rows)
        write_table("//tmp/t1", rows)

        map_reduce(
            in_="//tmp/t1",
            out="//tmp/t2",
            mapper_command="cat",
            reducer_command="cat",
            sort_by=["key"],
            spec={"pivot_keys": [[]]},
        )

        assert_items_equal(read_table("//tmp/t2"), sorted_dicts(rows))
        chunk_ids = get("//tmp/t2/@chunk_ids")
        assert sorted([get("#" + chunk_id + "/@row_count") for chunk_id in chunk_ids]) == [50]

        map_reduce(
            in_="//tmp/t1",
            out="//tmp/t3",
            reducer_command="cat",
            sort_by=["key"],
            spec={"pivot_keys": [[], ["25"]]},
        )

        assert_items_equal(read_table("//tmp/t3"), sorted_dicts(rows))
        chunk_ids = get("//tmp/t3/@chunk_ids")
        assert sorted([get("#" + chunk_id + "/@row_count") for chunk_id in chunk_ids]) == [25, 25]

    @authors("gritukan")
    def test_pivot_keys_incorrect_options(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        rows = [{"key": "%02d" % key} for key in range(50)]
        shuffle(rows)
        write_table("//tmp/t1", rows)

        with raises_yt_error("Pivot keys should should form"):
            map_reduce(
                in_="//tmp/t1",
                out="//tmp/t2",
                mapper_command="cat",
                reducer_command="cat",
                sort_by=["key"],
                spec={"pivot_keys": [["73"], ["37"]]},
            )

        with raises_yt_error("Pivot key cannot be longer"):
            map_reduce(
                in_="//tmp/t1",
                out="//tmp/t2",
                mapper_command="cat",
                reducer_command="cat",
                sort_by=["key"],
                spec={"pivot_keys": [["37", 42], ["73", 10]]},
            )

    @authors("gritukan")
    def test_pivot_keys_with_hierarchical_partitions(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        rows = [{"key": "%02d" % key} for key in range(50)]
        shuffle(rows)
        write_table("//tmp/t1", rows)

        map_reduce(
            in_="//tmp/t1",
            out="//tmp/t2",
            mapper_command="cat",
            reducer_command="cat",
            sort_by=["key"],
            spec={"pivot_keys": [["01"], ["22"], ["43"]], "max_partition_factor": 2},
        )
        assert_items_equal(read_table("//tmp/t2"), sorted_dicts(rows))
        chunk_ids = get("//tmp/t2/@chunk_ids")
        assert sorted([get("#" + chunk_id + "/@row_count") for chunk_id in chunk_ids]) == [1, 7, 21, 21]

    @authors("gritukan")
    def test_pivot_keys_descending(self):
        skip_if_no_descending(self.Env)

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")

        rows = [{"key": "%02d" % key} for key in range(50)]
        shuffle(rows)
        write_table("//tmp/t1", rows)

        sort_by = [{"name": "key", "sort_order": "descending"}]
        map_reduce(
            in_="//tmp/t1",
            out="//tmp/t2",
            mapper_command="cat",
            reducer_command="cat",
            sort_by=sort_by,
            spec={"pivot_keys": [["43"], ["01"]]},
        )

        assert_items_equal(read_table("//tmp/t2"), sorted_dicts(rows))
        chunk_ids = get("//tmp/t2/@chunk_ids")
        # Partitions are (+oo, 43), [43, 01), [01, -oo).
        assert sorted([get("#" + chunk_id + "/@row_count") for chunk_id in chunk_ids]) == [2, 6, 42]

        map_reduce(
            in_="//tmp/t1",
            out="//tmp/t3",
            reducer_command="cat",
            sort_by=sort_by,
            spec={"pivot_keys": [["43"], ["01"]]},
        )

        assert_items_equal(read_table("//tmp/t3"), sorted_dicts(rows))
        chunk_ids = get("//tmp/t3/@chunk_ids")
        # Partitions are (+oo, 43), [43, 01), [01, -oo).
        assert sorted([get("#" + chunk_id + "/@row_count") for chunk_id in chunk_ids]) == [2, 6, 42]

    @authors("levysotsky")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_intermediate_schema(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        schema = [
            {"name": "a", "type_v3": "int64", "sort_order": sort_order},
            {
                "name": "struct",
                "type_v3": struct_type(
                    [
                        ("a", "int64"),
                        ("b", "string"),
                        ("c", "bool"),
                    ]
                ),
            },
        ]
        output_schema = [
            {"name": "a", "type_v3": "int64"},
            {
                "name": "struct",
                "type_v3": struct_type(
                    [
                        ("a", "int64"),
                        ("b", "string"),
                        ("c", "bool"),
                    ]
                ),
            },
        ]

        create("table", "//tmp/t1")
        create("table", "//tmp/t2", attributes={"schema": output_schema})

        rows = [{"a": i, "b": str(i) * 3} for i in range(50)]
        write_table("//tmp/t1", rows)

        mapper = b"""
import sys, json
for l in sys.stdin:
    row = json.loads(l)
    a, b = row["a"], row["b"]
    sys.stdout.write(json.dumps({"a": a, "struct": {"a": a, "b": b, "c": True}}))
"""
        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        map_reduce(
            in_="//tmp/t1",
            out="//tmp/t2",
            mapper_file=["//tmp/mapper.py"],
            mapper_command="python mapper.py",
            reducer_command="cat",
            reduce_combiner_command="cat",
            sort_by=[{"name": "a", "sort_order": sort_order}],
            spec={
                "mapper": {
                    "output_streams": [
                        {"schema": schema},
                    ],
                    "format": "json",
                },
                "reducer": {"format": "json"},
                "reduce_combiner": {
                    "format": "json",
                },
                "max_failed_job_count": 1,
                "force_reduce_combiners": True,
            },
        )

        result_rows = read_table("//tmp/t2")
        expected_rows = [{"a": r["a"], "struct": {"a": r["a"], "b": r["b"], "c": True}} for r in rows]
        assert sorted_dicts(expected_rows) == sorted_dicts(result_rows)

    @authors("levysotsky")
    def test_single_intermediate_schema_trivial_mapper(self):
        input_schema = output_schema = [
            {"name": "a", "type_v3": "int64"},
            {
                "name": "struct",
                "type_v3": struct_type(
                    [
                        ("a", "int64"),
                        ("b", "string"),
                    ]
                ),
            },
        ]

        create("table", "//tmp/in", attributes={"schema": input_schema})
        create("table", "//tmp/out", attributes={"schema": output_schema})

        row_count = 5
        rows = [{"a": i, "struct": {"a": i ** 2, "b": str(i) * 3}} for i in range(row_count)]
        write_table("//tmp/in", rows)

        create("file", "//tmp/reducer.py")
        write_file("//tmp/reducer.py", self.DROP_TABLE_INDEX_REDUCER)

        map_reduce(
            in_=["//tmp/in"],
            out="//tmp/out",
            reducer_file=["//tmp/reducer.py"],
            reducer_command="python reducer.py",
            sort_by=[{"name": "a", "sort_order": "ascending"}],
            spec={
                "partition_count": 1,
                "data_size_per_map_job": 1,
                "data_size_per_sort_job": 1,
                "resource_limits": {"user_slots": 1},
                "reducer": {
                    "format": "json",
                },
                "max_failed_job_count": 1,
            },
        )

        result_rows = read_table("//tmp/out")
        expected_rows = rows
        assert sorted_dicts(result_rows) == sorted_dicts(expected_rows)

    @authors("levysotsky")
    @pytest.mark.parametrize("with_intermediate_sort", [True, False])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_several_intermediate_schemas_trivial_mapper(self, sort_order, with_intermediate_sort):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()
        is_compat = "22_1" in getattr(self, "ARTIFACT_COMPONENTS", {})
        if with_intermediate_sort and is_compat:
            pytest.xfail("Hasn't worked before")

        first_schema = [
            {"name": "a", "type_v3": "int64", "sort_order": sort_order},
            {
                "name": "struct",
                "type_v3": struct_type(
                    [
                        ("a", "int64"),
                        ("b", "string"),
                    ]
                ),
            },
        ]
        second_schema = [
            {"name": "a", "type_v3": "int64", "sort_order": sort_order},
            {
                "name": "struct1",
                "type_v3": struct_type(
                    [
                        ("a1", "int64"),
                    ]
                ),
            },
        ]
        output_schema = [
            {"name": "a", "type_v3": "int64"},
            {
                "name": "struct2",
                "type_v3": struct_type(
                    [
                        ("a2", "int64"),
                        ("b2", "string"),
                    ]
                ),
            },
        ]

        create("table", "//tmp/in1", attributes={"schema": first_schema})
        create("table", "//tmp/in2", attributes={"schema": second_schema})
        create("table", "//tmp/out", attributes={"schema": output_schema})

        row_count = 5
        rows1 = [{"a": i, "struct": {"a": i ** 2, "b": str(i) * 3}} for i in range(row_count)]
        if sort_order == "descending":
            rows1 = rows1[::-1]
        write_table("//tmp/in1", rows1)
        rows2 = [{"a": i, "struct1": {"a1": i ** 3}} for i in range(row_count)]
        if sort_order == "descending":
            rows2 = rows2[::-1]
        write_table("//tmp/in2", rows2)

        create("file", "//tmp/reducer.py")
        write_file("//tmp/reducer.py", self.TWO_INPUT_SCHEMAFUL_REDUCER)

        spec = {
            "reducer": {"format": "json"},
            "max_failed_job_count": 1,
        }

        if with_intermediate_sort:
            spec.update({
                "partition_job_count": 10,
                "partition_count": 10,
                "max_partition_factor": 4,
                "data_weight_per_sort_job": 1,
                "data_size_per_sort_job": 1,
                "data_weight_per_intermediate_partition_job": 10,
                "partition_job_io": {
                    "table_writer": {
                        "desired_chunk_size": 1,
                        "block_size": 1024,
                    }
                },
            })

        map_reduce(
            in_=["//tmp/in1", "//tmp/in2"],
            out="//tmp/out",
            reducer_file=["//tmp/reducer.py"],
            reducer_command="python reducer.py",
            sort_by=[{"name": "a", "sort_order": sort_order}],
            spec=spec,
        )

        result_rows = read_table("//tmp/out")
        expected_rows = []
        for a in range(row_count):
            row = {"a": a, "struct2": {"a2": a ** 2 + a ** 3, "b2": 3 * str(a)}}
            expected_rows.append(row)
        assert sorted_dicts(expected_rows) == sorted_dicts(result_rows)

    @authors("levysotsky")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_several_intermediate_schemas_trivial_mapper_type_casting(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        input_schemas = [
            [
                {"name": "a", "type_v3": "int64"},
                {
                    "name": "b",
                    "type_v3": struct_type(
                        [
                            ("x", "int64"),
                            ("y", optional_type("string")),
                        ]
                    ),
                },
            ],
            [
                {"name": "a", "type_v3": optional_type("int64")},
                {
                    "name": "b",
                    "type_v3": struct_type(
                        [
                            ("x", "int64"),
                        ]
                    ),
                },
            ],
            [
                {"name": "a", "type_v3": "int32"},
                {
                    "name": "b",
                    "type_v3": optional_type(struct_type(
                        [
                            ("x", optional_type("int64")),
                            ("y", optional_type("string")),
                            ("z", optional_type("int64")),
                        ]
                    )),
                },
            ],
        ]

        output_schema = [
            {"name": "a", "type_v3": optional_type("int64")},
            {
                "name": "b",
                "type_v3": optional_type(struct_type(
                    [
                        ("x", optional_type("int64")),
                        ("y", optional_type("string")),
                        ("z", optional_type("int64")),
                    ]
                )),
            },
        ]

        input_paths = ["//tmp/in_{}".format(i) for i in range(len(input_schemas))]
        for path, schema in zip(input_paths, input_schemas):
            create("table", path, attributes={"schema": schema})

        create("table", "//tmp/out", attributes={"schema": output_schema})

        row_count = 50
        all_rows = [[] for _ in input_schemas]
        rows0, rows1, rows2 = all_rows
        for i in range(row_count):
            rows0.append({
                "a": i,
                "b": {"x": i ** 2, "y": str(i) * 3 if i % 2 == 0 else None},
            })
            rows1.append({
                "a": row_count + i,
                "b": {"x": i ** 2},
            })
            rows2.append({
                "a": 2 * row_count + i,
                "b": {"x": i ** 2, "y": str(i) * 3 if i % 2 == 0 else None, "z": None}
            })
        for i, rows in enumerate(all_rows):
            write_table("//tmp/in_{}".format(i), rows)

        create("file", "//tmp/reducer.py")
        write_file("//tmp/reducer.py", self.DROP_TABLE_INDEX_REDUCER)

        map_reduce(
            in_=input_paths,
            out="//tmp/out",
            reducer_file=["//tmp/reducer.py"],
            reducer_command="python reducer.py",
            sort_by=[
                {"name": "a", "sort_order": sort_order},
                {"name": "b", "sort_order": sort_order},
            ],
            spec={
                "reducer": {"format": "json"},
                "max_failed_job_count": 1,
            },
        )

        result_rows = list(read_table("//tmp/out"))
        assert len(result_rows) == row_count * len(input_schemas)
        assert sorted([r["a"] for r in result_rows]) == list(range(row_count * len(input_schemas)))

    @authors("aleexfi")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    @pytest.mark.parametrize(
        "method",
        [
            "map_reduce",
            "map_reduce_1p",
            "map_reduce_with_hierarchical_partitions",
            "ordered_map_reduce",
        ],
    )
    def test_several_intermediate_schemas_passing(self, sort_order, method):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        is_compat = "22_4" in getattr(self, "ARTIFACT_COMPONENTS", {})
        if is_compat and method == "ordered_map_reduce":
            pytest.xfail("Hasn't worked before")

        first_schema = [
            {"name": "a", "type_v3": "int64", "sort_order": sort_order},
            {
                "name": "struct",
                "type_v3": struct_type(
                    [
                        ("a", "int64"),
                        ("b", "string"),
                    ]
                ),
            },
        ]
        second_schema = [
            {"name": "a", "type_v3": "int64", "sort_order": sort_order},
            {
                "name": "struct1",
                "type_v3": struct_type(
                    [
                        ("a1", "int64"),
                    ]
                ),
            },
        ]
        output_schema = [
            {"name": "a", "type_v3": "int64"},
            {
                "name": "struct2",
                "type_v3": struct_type(
                    [
                        ("a2", "int64"),
                        ("b2", "string"),
                    ]
                ),
            },
        ]

        create("table", "//tmp/t1")
        create("table", "//tmp/t2", attributes={"schema": output_schema})

        row_count = 50
        rows = [{"a": i, "b": str(i) * 3} for i in range(row_count)]
        write_table("//tmp/t1", rows)

        mapper = b"""
import os, sys, json
for l in sys.stdin:
    row = json.loads(l)
    a, b = row["a"], row["b"]
    if a % 2 == 0:
        out_row = {"a": a // 2, "struct": row}
        os.write(1, json.dumps(out_row) + "\\n")
    else:
        out_row = {"a": a // 2, "struct1": {"a1": a - 1}}
        os.write(4, json.dumps(out_row) + "\\n")
"""
        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        create("file", "//tmp/reducer.py")
        write_file("//tmp/reducer.py", self.TWO_INPUT_SCHEMAFUL_REDUCER)

        if method == "map_reduce":
            map_reduce(
                in_="//tmp/t1",
                out="//tmp/t2",
                mapper_file=["//tmp/mapper.py"],
                mapper_command="python mapper.py",
                reducer_file=["//tmp/reducer.py"],
                reducer_command="python reducer.py",
                sort_by=[{"name": "a", "sort_order": sort_order}],
                spec={
                    "mapper": {
                        "output_streams": [
                            {"schema": first_schema},
                            {"schema": second_schema},
                        ],
                        "format": "json",
                    },
                    "reducer": {"format": "json"},
                    "max_failed_job_count": 1,
                    "time_limit": 60000,
                },
            )
        elif method == "map_reduce_1p":
            map_reduce(
                in_="//tmp/t1",
                out="//tmp/t2",
                mapper_file=["//tmp/mapper.py"],
                mapper_command="python mapper.py",
                reducer_file=["//tmp/reducer.py"],
                reducer_command="python reducer.py",
                sort_by=[{"name": "a", "sort_order": sort_order}],
                spec={
                    "partition_count": 1,
                    "mapper": {
                        "output_streams": [
                            {"schema": first_schema},
                            {"schema": second_schema},
                        ],
                        "format": "json",
                    },
                    "reducer": {"format": "json"},
                    "max_failed_job_count": 1,
                    "time_limit": 60000,
                },
            )
        elif method == "ordered_map_reduce":
            map_reduce(
                in_="//tmp/t1",
                out="//tmp/t2",
                mapper_file=["//tmp/mapper.py"],
                mapper_command="python mapper.py",
                reducer_file=["//tmp/reducer.py"],
                reducer_command="python reducer.py",
                sort_by=[{"name": "a", "sort_order": sort_order}],
                spec={
                    "partition_count": 2,
                    "map_job_count": 2,
                    "data_size_per_sort_job": 10,
                    "ordered": True,
                    "mapper": {
                        "output_streams": [
                            {"schema": first_schema},
                            {"schema": second_schema},
                        ],
                        "format": "json",
                    },
                    "reducer": {"format": "json"},
                    "max_failed_job_count": 1,
                    "time_limit": 60000,
                },
            )
        elif method == "map_reduce_with_hierarchical_partitions":
            map_reduce(
                in_="//tmp/t1",
                out="//tmp/t2",
                mapper_file=["//tmp/mapper.py"],
                mapper_command="python mapper.py",
                reducer_file=["//tmp/reducer.py"],
                reducer_command="python reducer.py",
                sort_by=[{"name": "a", "sort_order": sort_order}],
                spec={
                    "partition_count": 7,
                    "max_partition_factor": 2,
                    "map_job_count": 2,
                    "data_size_per_sort_job": 10,
                    "mapper": {
                        "output_streams": [
                            {"schema": first_schema},
                            {"schema": second_schema},
                        ],
                        "format": "json",
                    },
                    "reducer": {"format": "json"},
                    "max_failed_job_count": 1,
                    "time_limit": 60000,
                },
            )
        else:
            assert False

        result_rows = read_table("//tmp/t2")
        expected_rows = []
        for a in range(row_count // 2):
            row = {"a": a, "struct2": {"a2": 4 * a, "b2": 3 * str(2 * a)}}
            expected_rows.append(row)
        assert sorted_dicts(expected_rows) == sorted_dicts(result_rows)

    @authors("levysotsky")
    @pytest.mark.parametrize("cat_combiner", [False, True])
    @pytest.mark.xfail(reason="several streams with combiner are not currently supported")
    def test_several_intermediate_schemas_with_combiner(self, cat_combiner):
        first_schema = [
            {"name": "a", "type_v3": "int64", "sort_order": "ascending"},
            {
                "name": "struct",
                "type_v3": struct_type(
                    [
                        ("a", "int64"),
                        ("b", "string"),
                    ]
                ),
            },
        ]
        second_schema = [
            {"name": "a", "type_v3": "int64", "sort_order": "ascending"},
            {
                "name": "struct1",
                "type_v3": struct_type(
                    [
                        ("a1", "int64"),
                    ]
                ),
            },
        ]
        output_schema = [
            {"name": "a", "type_v3": "int64"},
            {
                "name": "struct2",
                "type_v3": struct_type(
                    [
                        ("a2", "int64"),
                        ("b2", "string"),
                    ]
                ),
            },
        ]

        create("table", "//tmp/t1")
        create("table", "//tmp/t2", attributes={"schema": output_schema})

        row_count = 50
        rows = [{"a": i, "b": str(i) * 3} for i in range(row_count)]
        write_table("//tmp/t1", rows)

        mapper = b"""
import os, sys, json
for l in sys.stdin:
    row = json.loads(l)
    a, b = row["a"], row["b"]
    if a % 2 == 0:
        out_row = {"a": a // 2, "struct": row}
        os.write(1, json.dumps(out_row) + "\\n")
    else:
        out_row = {"a": a // 2, "struct1": {"a1": a - 1}}
        os.write(4, json.dumps(out_row) + "\\n")
"""
        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        reduce_combiner = b"""
import os, sys, json
table_index = 0
for l in sys.stdin:
    row = json.loads(l)
    if "$attributes" in row:
        assert row["$value"] is None
        table_index = row["$attributes"]["table_index"]
        continue
    if table_index == 0:
        row["struct"]["b"] += "_after_combiner"
    else:
        assert table_index == 1
        row["struct1"]["a1"] += 100
    os.write(table_index * 3 + 1, json.dumps(row))
"""
        create("file", "//tmp/reduce_combiner.py")
        write_file("//tmp/reduce_combiner.py", reduce_combiner)

        create("file", "//tmp/reducer.py")
        write_file("//tmp/reducer.py", self.TWO_INPUT_SCHEMAFUL_REDUCER)

        map_reduce(
            in_="//tmp/t1",
            out="//tmp/t2",
            mapper_file=["//tmp/mapper.py"],
            mapper_command="python mapper.py",
            reduce_combiner_file=["//tmp/reduce_combiner.py"],
            reduce_combiner_command="cat" if cat_combiner else "python reduce_combiner.py",
            reducer_file=["//tmp/reducer.py"],
            reducer_command="python reducer.py",
            sort_by=["a"],
            spec={
                "mapper": {
                    "output_streams": [
                        {"schema": first_schema},
                        {"schema": second_schema},
                    ],
                    "format": "json",
                },
                "reducer": {"format": "json"},
                "reduce_combiner": {"format": "json"},
                "force_reduce_combiners": True,
                "max_failed_job_count": 1,
            },
        )

        result_rows = read_table("//tmp/t2")
        expected_rows = []
        for a in range(row_count // 2):
            if cat_combiner:
                row = {"a": a, "struct2": {"a2": 4 * a, "b2": 3 * str(2 * a)}}
            else:
                row = {
                    "a": a,
                    "struct2": {
                        "a2": 4 * a + 100,
                        "b2": 3 * str(2 * a) + "_after_combiner",
                    },
                }
            expected_rows.append(row)
        assert sorted_dicts(expected_rows) == sorted_dicts(result_rows)

    @authors("levysotsky")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_identical_intermediate_schemas(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        schema = [
            {"name": "a", "type_v3": "int64", "sort_order": sort_order},
            {
                "name": "struct",
                "type_v3": struct_type(
                    [
                        ("a", "int64"),
                        ("b", "string"),
                    ]
                ),
            },
        ]
        output_schema = [
            {"name": "a", "type_v3": "int64"},
            {
                "name": "struct2",
                "type_v3": struct_type(
                    [
                        ("a2", "int64"),
                        ("b2", "string"),
                    ]
                ),
            },
        ]

        create("table", "//tmp/t1")
        create("table", "//tmp/t2", attributes={"schema": output_schema})

        row_count = 50
        rows = [{"a": i, "b": str(i) * 3} for i in range(row_count)]
        write_table("//tmp/t1", rows)

        mapper = b"""
import os, sys, json
for l in sys.stdin:
    row = json.loads(l)
    a, b = row["a"], row["b"]
    out_row = {"a": a, "struct": {"a": a**2, "b": str(a) * 3}}
    os.write(1, json.dumps(out_row) + "\\n")
    out_row = {"a": a, "struct": {"a": a**3, "b": str(a) * 5}}
    os.write(4, json.dumps(out_row) + "\\n")
"""
        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper)

        create("file", "//tmp/reducer.py")
        write_file("//tmp/reducer.py", self.TWO_INPUT_SCHEMAFUL_REDUCER_IDENTICAL_SCHEMAS)

        map_reduce(
            in_="//tmp/t1",
            out="//tmp/t2",
            mapper_file=["//tmp/mapper.py"],
            mapper_command="python mapper.py",
            reducer_file=["//tmp/reducer.py"],
            reducer_command="python reducer.py",
            sort_by=[{"name": "a", "sort_order": sort_order}],
            spec={
                "mapper": {
                    "output_streams": [
                        {"schema": schema},
                        {"schema": schema},
                    ],
                    "format": "json",
                },
                "reducer": {"format": "json"},
                "max_failed_job_count": 1,
            },
        )

        result_rows = read_table("//tmp/t2")
        expected_rows = []
        for a in range(row_count):
            row = {"a": a, "struct2": {"a2": a ** 2 + a ** 3, "b2": 3 * str(a)}}
            expected_rows.append(row)
        assert sorted_dicts(expected_rows) == sorted_dicts(result_rows)

    @authors("levysotsky")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_identical_intermediate_schemas_trivial_mapper(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        input_schema = [
            {"name": "a", "type_v3": "int64", "sort_order": sort_order},
            {
                "name": "struct",
                "type_v3": struct_type(
                    [
                        ("a", "int64"),
                        ("b", "string"),
                    ]
                ),
            },
        ]
        output_schema = [
            {"name": "a", "type_v3": "int64"},
            {
                "name": "struct2",
                "type_v3": struct_type(
                    [
                        ("a2", "int64"),
                        ("b2", "string"),
                    ]
                ),
            },
        ]

        create("table", "//tmp/in1", attributes={"schema": input_schema})
        create("table", "//tmp/in2", attributes={"schema": input_schema})
        create("table", "//tmp/out", attributes={"schema": output_schema})

        row_count = 50
        rows1 = [{"a": i, "struct": {"a": i ** 2, "b": str(i) * 3}} for i in range(row_count)]
        if sort_order == "descending":
            rows1 = rows1[::-1]
        write_table("//tmp/in1", rows1)
        rows2 = [{"a": i, "struct": {"a": i ** 3, "b": str(i) * 5}} for i in range(row_count)]
        if sort_order == "descending":
            rows2 = rows2[::-1]
        write_table("//tmp/in2", rows2)

        create("file", "//tmp/reducer.py")
        write_file("//tmp/reducer.py", self.TWO_INPUT_SCHEMAFUL_REDUCER_IDENTICAL_SCHEMAS)

        map_reduce(
            in_=["//tmp/in1", "//tmp/in2"],
            out="//tmp/out",
            reducer_file=["//tmp/reducer.py"],
            reducer_command="python reducer.py",
            sort_by=[{"name": "a", "sort_order": sort_order}],
            spec={
                "reducer": {"format": "json"},
                "max_failed_job_count": 1,
            },
        )

        result_rows = read_table("//tmp/out")
        expected_rows = []
        for a in range(row_count):
            row = {"a": a, "struct2": {"a2": a ** 2 + a ** 3, "b2": 3 * str(a)}}
            expected_rows.append(row)
        assert sorted_dicts(expected_rows) == sorted_dicts(result_rows)

    @authors("levysotsky")
    def test_several_intermediate_schemas_composite_in_key(self):
        key_columns = [
            {
                "name": "key1",
                "type_v3": tuple_type(["int64", "string"]),
                "sort_order": "ascending",
            },
            {"name": "key2", "type_v3": list_type("int64"), "sort_order": "ascending"},
            {"name": "key3", "type_v3": "string", "sort_order": "ascending"},
        ]
        first_schema = key_columns + [
            {
                "name": "struct",
                "type_v3": struct_type(
                    [
                        ("a", "int64"),
                        ("b", "string"),
                    ]
                ),
            },
        ]
        second_schema = key_columns + [
            {
                "name": "struct1",
                "type_v3": struct_type(
                    [
                        ("a1", "int64"),
                        ("list1", list_type("int64")),
                    ]
                ),
            },
        ]
        output_schema = [
            {"name": "key1", "type_v3": tuple_type(["int64", "string"])},
            {
                "name": "struct2",
                "type_v3": struct_type(
                    [
                        ("a2", "int64"),
                        ("b2", "string"),
                        ("list2", list_type("int64")),
                    ]
                ),
            },
        ]

        create("table", "//tmp/t1")
        create("table", "//tmp/t2", attributes={"schema": output_schema})

        row_count = 50
        rows = [{"a": i} for i in range(row_count)]
        write_table("//tmp/t1", rows)

        mapper = """
import os, sys, json
row_count = {row_count}
for l in sys.stdin:
    a = json.loads(l)["a"]
    key = {{
        "key1": [(row_count - a - 1) // 10, str(a)],
        "key2": [a] * (a // 10),
        "key3": str(a) * 3,
    }}
    out_row_1 = {{
        "struct": {{
            "a": a,
            "b": str(a) * 3,
        }},
    }}
    out_row_1.update(key)
    os.write(1, json.dumps(out_row_1) + "\\n")
    out_row_2 = {{
        "struct1": {{
            "a1": a,
            "list1": [a] * (a // 10),
        }},
    }}
    out_row_2.update(key)
    os.write(4, json.dumps(out_row_2) + "\\n")
"""
        create("file", "//tmp/mapper.py")
        write_file("//tmp/mapper.py", mapper.format(row_count=row_count).encode())

        reducer = b"""
import os, sys, json
table_index = 0
rows_got = 0
for l in sys.stdin:
    row = json.loads(l)
    if "$attributes" in row:
        assert row["$value"] is None
        table_index = row["$attributes"]["table_index"]
        continue
    if rows_got == 0:
        key2_sum = 0
    key2_sum += sum(row["key2"])
    if table_index == 0:
        b = row["struct"]["b"]
    else:
        assert table_index == 1
        list2 = row["struct1"]["list1"]
    rows_got += 1
    if rows_got == 2:
        out_row = {
            "key1": row["key1"],
            "struct2": {
                "a2": key2_sum,
                "b2": b,
                "list2": list2,
            },
        }
        sys.stdout.write(json.dumps(out_row) + "\\n")
        rows_got = 0
"""

        create("file", "//tmp/reducer.py")
        write_file("//tmp/reducer.py", reducer)

        map_reduce(
            in_="//tmp/t1",
            out="//tmp/t2",
            mapper_file=["//tmp/mapper.py"],
            mapper_command="python mapper.py",
            reducer_file=["//tmp/reducer.py"],
            reducer_command="python reducer.py",
            sort_by=["key1", "key2", "key3"],
            spec={
                "mapper": {
                    "output_streams": [
                        {"schema": first_schema},
                        {"schema": second_schema},
                    ],
                    "format": "json",
                },
                "reducer": {"format": "json"},
                "max_failed_job_count": 1,
            },
        )

        result_rows = read_table("//tmp/t2")
        expected_rows = []
        for a in range(row_count):
            row = {
                "key1": [(row_count - a - 1) // 10, str(a)],
                "struct2": {
                    "a2": 2 * a * (a // 10),
                    "b2": str(a) * 3,
                    "list2": [a] * (a // 10),
                },
            }
            expected_rows.append(row)
        assert sorted_dicts(expected_rows) == sorted_dicts(result_rows)

    @authors("levysotsky")
    def test_distinct_intermediate_schemas_trivial_mapper_json(self):
        first_schema = [
            {"name": "key1", "type_v3": tuple_type(["int64", "string"])},
            {"name": "key3", "type_v3": "string"},
            {
                "name": "struct",
                "type_v3": struct_type(
                    [
                        ("a", "int64"),
                        ("b", "string"),
                    ]
                ),
            },
        ]
        second_schema = [
            {"name": "key2", "type_v3": list_type("int64")},
            {"name": "key3", "type_v3": "string"},
            {
                "name": "struct1",
                "type_v3": struct_type(
                    [
                        ("a1", "int64"),
                        ("list1", list_type("int64")),
                    ]
                ),
            },
        ]
        output_schema = [
            {"name": "key1", "type_v3": tuple_type(["int64", "string"])},
            {
                "name": "struct2",
                "type_v3": struct_type(
                    [
                        ("a2", "int64"),
                        ("b2", "string"),
                        ("list2", list_type("int64")),
                    ]
                ),
            },
        ]

        create("table", "//tmp/in1", attributes={"schema": first_schema})
        create("table", "//tmp/in2", attributes={"schema": second_schema})
        create("table", "//tmp/out", attributes={"schema": output_schema})

        row_count = 50
        first_rows = [
            {
                "key1": [(row_count - a - 1) // 10, str(a)],
                "key3": str(a) * 3,
                "struct": {
                    "a": a,
                    "b": str(a) * 3,
                },
            }
            for a in range(row_count)
        ]
        shuffle(first_rows)
        write_table("//tmp/in1", first_rows)
        second_rows = [
            {
                "key2": [a] * (a // 10),
                "key3": str(a) * 3,
                "struct1": {
                    "a1": a,
                    "list1": [a] * (a // 10),
                },
            }
            for a in range(row_count)
        ]
        shuffle(second_rows)
        write_table("//tmp/in2", second_rows)

        reducer = b"""
import os, sys, json
key2_sum = 0
list2 = []
first_batch_sent = False
for l in sys.stdin:
    row = json.loads(l)
    if "$attributes" in row:
        assert row["$value"] is None
        table_index = row["$attributes"]["table_index"]
        continue
    if row.get("key1") is None:
        assert table_index == 1
        list2 += row["struct1"]["list1"]
        key2_sum += sum(row["key2"])
    else:
        if not first_batch_sent:
            out_row = {
                "key1": [-111, "-111"],
                "struct2": {
                    "a2": key2_sum,
                    "b2": "nothing",
                    "list2": list2,
                },
            }
            sys.stdout.write(json.dumps(out_row) + "\\n")
            first_batch_sent = True
        b = row["struct"]["b"]
        assert table_index == 0
        out_row = {
            "key1": row.get("key1"),
            "struct2": {
                "a2": 0,
                "b2": b,
                "list2": [],
            },
        }
        sys.stdout.write(json.dumps(out_row) + "\\n")
"""
        create("file", "//tmp/reducer.py")
        write_file("//tmp/reducer.py", reducer)

        map_reduce(
            in_=["//tmp/in1", "//tmp/in2"],
            out="//tmp/out",
            reducer_file=["//tmp/reducer.py"],
            reducer_command="python reducer.py",
            sort_by=["key1", "key2", "key3"],
            spec={
                "reducer": {"format": "json"},
                "max_failed_job_count": 1,
            },
        )

        result_rows = read_table("//tmp/out")
        expected_rows = []
        for a in range(row_count):
            row_1 = {
                "key1": [(row_count - a - 1) // 10, str(a)],
                "struct2": {
                    "a2": 0,
                    "b2": str(a) * 3,
                    "list2": [],
                },
            }
            expected_rows.append(row_1)
        row_2 = {
            "key1": [-111, "-111"],
            "struct2": {
                "a2": sum(a * (a // 10) for a in range(row_count)),
                "b2": "nothing",
                "list2": sum(([a] * (a // 10) for a in range(row_count)), []),
            },
        }
        expected_rows.append(row_2)
        assert sorted_dicts(expected_rows) == sorted_dicts(result_rows)

    @authors("levysotsky")
    def test_wrong_intermediate_schemas(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        rows = [{"a": i, "b": str(i) * 3} for i in range(50)]
        write_table("//tmp/t1", rows)

        create("file", "//tmp/reducer.py")
        write_file("//tmp/reducer.py", self.DROP_TABLE_INDEX_REDUCER)

        def run(sort_by, schemas):
            map_reduce(
                in_="//tmp/t1",
                out="//tmp/t2",
                mapper_command="cat",
                reducer_file=["//tmp/reducer.py"],
                reducer_command="python reducer.py",
                sort_by=sort_by,
                spec={
                    "mapper": {
                        "format": "json",
                        "output_streams": [{"schema": schema} for schema in schemas],
                    },
                    "reducer": {"format": "json"},
                    "max_failed_job_count": 1,
                },
            )

        with pytest.raises(YtError):
            # Key column types must not differ.
            run(
                ["a", "b"],
                [
                    [
                        {"name": "a", "type_v3": "int64", "sort_order": "ascending"},
                        {"name": "b", "type_v3": "bool", "sort_order": "ascending"},
                    ],
                    [
                        {"name": "a", "type_v3": "int64", "sort_order": "ascending"},
                        {"name": "b", "type_v3": "string", "sort_order": "ascending"},
                    ],
                ],
            )

        # Non-key columns can differ.
        run(
            ["a"],
            [
                [
                    {"name": "a", "type_v3": "int64", "sort_order": "ascending"},
                    {"name": "b", "type_v3": "bool"},
                ],
                [
                    {"name": "a", "type_v3": "int64", "sort_order": "ascending"},
                    {"name": "b", "type_v3": "string"},
                ],
            ],
        )

        with pytest.raises(YtError):
            # Key columns must correspond to sort_by.
            run(
                ["a"],
                [
                    [
                        {"name": "a", "type_v3": "int64", "sort_order": "ascending"},
                        {"name": "b", "type_v3": "bool", "sort_order": "ascending"},
                    ],
                    [
                        {"name": "a", "type_v3": "int64", "sort_order": "ascending"},
                        {"name": "b", "type_v3": "bool", "sort_order": "ascending"},
                    ],
                ],
            )

    @authors("levysotsky")
    def test_wrong_intermediate_schemas_trivial_mapper(self):
        create("file", "//tmp/reducer.py")
        write_file("//tmp/reducer.py", self.DROP_TABLE_INDEX_REDUCER)

        def run(sort_by, schemas, rows):
            inputs = ["//tmp/in{}".format(i) for i in range(len(schemas))]
            for path, schema, table_rows in zip(inputs, schemas, rows):
                remove(path, force=True)
                create("table", path, attributes={"schema": schema})
                write_table(path, table_rows)
            remove("//tmp/out", force=True)
            create("table", "//tmp/out")
            map_reduce(
                in_=inputs,
                out="//tmp/out",
                reducer_file=["//tmp/reducer.py"],
                reducer_command="python reducer.py",
                sort_by=sort_by,
                spec={
                    "reducer": {"format": "json"},
                    "max_failed_job_count": 1,
                },
            )

        with pytest.raises(YtError):
            # Key column types must be castable.
            run(
                ["a", "b"],
                [
                    [
                        {"name": "a", "type_v3": "int64"},
                        {"name": "b", "type_v3": "bool"},
                    ],
                    [
                        {"name": "a", "type_v3": "int64"},
                        {"name": "b", "type_v3": "string"},
                    ],
                ],
                [
                    [{"a": 10, "b": False}],
                    [{"a": 12, "b": "NotBool"}],
                ],
            )

        with pytest.raises(YtError):
            # Key column types must be castable.
            run(
                ["a", "b"],
                [
                    [
                        {"name": "a", "type_v3": "int64"},
                        {"name": "b", "type_v3": "int64"},
                    ],
                    [
                        {"name": "a", "type_v3": "uint64"},
                        {"name": "b", "type_v3": "int64"},
                    ],
                ],
                [
                    [{"a": -10, "b": -20}],
                    [{"a": 12, "b": -20}],
                ],
            )

        # Key column types don't have to be identical.
        run(
            ["a", "b"],
            [
                [
                    {"name": "a", "type_v3": "int64"},
                    {"name": "b", "type_v3": optional_type("bool")},
                ],
                [
                    {"name": "a", "type_v3": optional_type("int64")},
                    {"name": "b", "type_v3": "bool"},
                ],
            ],
            [
                [{"a": 10, "b": None}],
                [{"a": None, "b": True}],
            ],
        )

        # Key column types don't have to be identical.
        run(
            ["a", "b"],
            [
                [
                    {"name": "a", "type_v3": "int32"},
                    {"name": "b", "type_v3": optional_type("string")},
                ],
                [
                    {"name": "a", "type_v3": optional_type("int64")},
                    {"name": "b", "type_v3": "utf8"},
                ],
            ],
            [
                [{"a": 10, "b": None}],
                [{"a": None, "b": b"\xd0\xa3\xd1\x85"}],
            ],
        )

        # Non-key columns can differ.
        run(
            ["a"],
            [
                [
                    {"name": "a", "type_v3": "int64"},
                    {"name": "b", "type_v3": "bool"},
                ],
                [
                    {"name": "a", "type_v3": "int64"},
                    {"name": "b", "type_v3": "string"},
                ],
            ],
            [
                [{"a": 10, "b": False}],
                [{"a": 12, "b": "NotBool"}],
            ],
        )

        # Key columns can intersect freely.
        run(
            ["a", "b", "c"],
            [
                [
                    {"name": "a", "type_v3": "int64"},
                    {"name": "b", "type_v3": "bool"},
                    {"name": "d", "type_v3": "string"},
                ],
                [
                    {"name": "b", "type_v3": "bool"},
                    {"name": "c", "type_v3": "string"},
                    {"name": "d", "type_v3": "double"},
                ],
            ],
            [
                [{"a": 10, "b": False, "d": "blh"}],
                [{"b": True, "c": "booh", "d": 2.71}],
            ],
        )

    @authors("gritukan")
    def test_job_splitting(self):
        pytest.skip("Job splitting + lost jobs = no way.")
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        expected = []
        for i in range(20):
            row = {"a": str(i), "b": "x" * 10**6}
            write_table("<append=%true>//tmp/t_in", row)
            expected.append({"a": str(i)})

        slow_cat = """
while read ROW; do
    if [ "$YT_JOB_COOKIE" == 0 ]; then
        sleep 5
    else
        sleep 0.1
    fi
    echo "$ROW"
done
"""
        op = map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            mapper_command=slow_cat,
            reducer_command="cat",
            sort_by=["key"],
            spec={
                "mapper": {"format": "dsv"},
                "reducer": {"format": "dsv"},
                "data_size_per_map_job": 14 * 1024 * 1024,
                "partition_count": 2,
                "map_job_io": {
                    "buffer_row_count": 1,
                },
            })

        assert get(op.get_path() + "/controller_orchid/progress/tasks/0/task_name") == "partition_map(0)"

        path = op.get_path() + "/controller_orchid/progress/tasks/0/job_counter/completed/interrupted/job_split"
        assert get(path) == 1

        assert sorted_dicts(read_table("//tmp/t_out{a}", verbose=False)) == sorted_dicts(expected)

    @authors("gritukan")
    def test_job_speculation(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        expected = []
        for i in range(20):
            row = {"a": str(i), "b": "x" * 10**6}
            write_table("<append=%true>//tmp/t_in", row)
            expected.append({"a": str(i)})

        mapper = """
while read ROW; do
    if [ "$YT_JOB_INDEX" == 0 ]; then
        sleep 5
    else
        sleep 0.1
    fi
    echo "$ROW"
done
"""
        op = map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            mapper_command=mapper,
            reducer_command="cat",
            sort_by=["a"],
            spec={
                "mapper": {"format": "dsv"},
                "reducer": {"format": "dsv"},
                "data_size_per_map_job": 14 * 1024 * 1024,
                "partition_count": 2,
                "enable_job_splitting": False,
                "reduce_job_io": {
                    "testing_options": {"pipe_delay": 1000},
                    "buffer_row_count": 1,
                }
            })
        op.track()

        assert get(op.get_path() + "/@progress/tasks/0/task_name") == "partition_map(0)"

        path = op.get_path() + "/@progress/tasks/0/speculative_job_counter/aborted/scheduled/speculative_run_won"
        assert get(path) == 1

    @authors("gritukan")
    def test_empty_mapper_output(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", {"foo": "bar"})

        map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            mapper_command="true",
            reducer_command="cat",
            sort_by=["foo"],
            spec={
                "mapper": {"format": "dsv"},
                "reducer": {"format": "dsv"},
            })

        assert get("//tmp/t_out/@chunk_count") == 0

    @authors("gritukan")
    def test_empty_sort_by(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", {"foo": "bar"})

        with pytest.raises(YtError):
            map_reduce(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                mapper_command="cat",
                reducer_command="cat")

    @authors("gritukan")
    def test_longer_sort_columns_sorted_reduce(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        expected = []
        for i in range(20):
            row = {"a": "a", "b": "%02d" % (20 - i)}
            write_table("<append=%true>//tmp/t_in", row)
            expected.append(row)
        expected = expected[::-1]

        map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            mapper_command="cat",
            reducer_command="cat",
            reduce_by=["a"],
            sort_by=["a", "b"],
            spec={
                "mapper": {"format": "dsv"},
                "reducer": {"format": "dsv"},
                "data_size_per_map_job": 1,
                "data_size_per_sort_job": 1,
            })

        assert read_table("//tmp/t_out") == expected

    @authors("max42")
    @pytest.mark.parametrize("stage", ["partition_map", "reduce_combiner", "sorted_reduce"])
    def test_data_weight_per_sorted_job_limit_exceeded(self, stage):
        create("table", "//tmp/t_in")

        light_row = "A"
        heavy_row = "A" * 20
        echo_heavy_row = "echo '{x=" + heavy_row + "}'"
        cat = "cat"

        write_table("//tmp/t_in", [{"x": heavy_row if stage == "partition_map" else light_row}])

        create("table", "//tmp/t_out")

        try:
            map_reduce(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                mapper_command=cat,
                reduce_combiner_command=echo_heavy_row if stage == "reduce_combiner" else cat,
                reducer_command=echo_heavy_row if stage == "sorted_reduce" else cat,
                reduce_by=["x"],
                format="<format=text>yson",
                spec={
                    "force_reduce_combiners": True,
                    "max_data_weight_per_job": 10,
                }
            )
        except YtError as err:
            assert "Exception thrown in operation controller" not in str(err)

    @authors("gepardo")
    def test_yt17852(self):
        if self.Env.get_component_version("ytserver-controller-agent").abi <= (22, 3):
            pytest.skip()

        create("table", "//tmp/in",
               attributes={"schema": [{"name": "a", "type": "int64"}]})
        write_table("//tmp/in", [{"a": 1}, {"a": 42}, {"a": 84}])
        create("table", "//tmp/out")

        with raises_yt_error("Error parsing operation spec"):
            map_reduce(
                in_="//tmp/in",
                out="//tmp/out",
                reducer_command="cat",
                reduce_by=["a"],
                spec={
                    "mapper_output_table_count": 2,
                }
            )

    @authors("galtsev")
    def test_no_segfault_after_abandon_job(self):
        if self.Env.get_component_version("ytserver-controller-agent").abi < (23, 1):
            pytest.skip("In versions less than 23.1 the controller agent segfaults after an abandon job request")

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"x": 1, "y": 2}, {"x": 2, "y": 3}] * 5)

        op = map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            reduce_by="x",
            sort_by="x",
            reducer_command=with_breakpoint("cat; BREAKPOINT"),
            track=False,
        )

        job_id = wait_breakpoint()[0]
        abandon_job(job_id)

        release_breakpoint()
        op.track()

    @pytest.mark.xfail(reason="YT-19087 is not closed")
    @authors("ermolovd")
    def test_column_filter_intermediate_schema_YT_19087(self):
        create("table", "//tmp/t_in", attributes={
            "schema": [
                make_column("a", "string"),
                make_column("b", "string"),
            ]})

        create("table", "//tmp/t_out")
        write_table("//tmp/t_in", [
            {"a": "a one", "b": "b one"},
            {"a": "a two", "b": "b two"},
        ])

        map_reduce(
            in_="//tmp/t_in{b}",
            out="//tmp/t_out",
            reduce_by="b",
            reducer_command="cat",
            spec={
                "reducer": {
                    "enable_input_table_index": True
                },
            },
        )

        assert read_table("//tmp/t_out") == [
            {"b": "b one"},
            {"b": "b two"},
        ]

    @authors("coteeq")
    def test_mapper_does_not_duplicate_lost_chunk(self):
        if self.Env.get_component_version("ytserver-controller-agent").abi <= (23, 2):
            pytest.skip()

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        create("table", "//tmp/t_out_mapper")

        orig_data = [{"x": 1, "y": 2}, {"x": 2, "y": 3}] * 5
        write_table("//tmp/t_in", orig_data)

        op = map_reduce(
            in_="//tmp/t_in",
            out=[
                "//tmp/t_out_mapper",
                "//tmp/t_out",
            ],
            reduce_by="x",
            sort_by="x",
            mapper_command="tee /proc/self/fd/4",
            reducer_command=with_breakpoint("cat;BREAKPOINT"),
            spec={
                "sort_locality_timeout": 0,
                "sort_assignment_timeout": 0,
                "enable_partitioned_data_balancing": False,
                "intermediate_data_replication_factor": 1,
                "sort_job_io": {"table_reader": {"retry_count": 1, "pass_count": 1}},
                "mapper_output_table_count": 1,
            },
            track=False,
        )

        first_reduce_job = wait_breakpoint()[0]

        assert self._ban_nodes_with_intermediate_chunks() != []
        self._abort_single_job_if_running_after_node_ban(op, first_reduce_job)

        release_breakpoint()
        op.track()

        assert get(op.get_path() + "/@progress/map_jobs/lost") == 1

        assert_items_equal(read_table("//tmp/t_out_mapper"), orig_data)
        assert_items_equal(read_table("//tmp/t_out"), orig_data)

    @authors("coteeq")
    @pytest.mark.xfail(
        reason=(
            "Restarted job pushes recalculated chunks only to those chunk pools, which reported "
            "chunk loss. This leads to inconsistency between mapper's and reducer's outputs."
            "In this test, intermediate chunk is lost and recalculated, but"
            "mapper's output chunk is still alive and has old data from lost job"
        )
    )
    def test_mapper_does_not_duplicate_lost_chunk_consistent_restart(self):
        if self.Env.get_component_version("ytserver-controller-agent").abi <= (23, 2):
            pytest.skip()
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        create("table", "//tmp/t_out_mapper")

        orig_data = [{"x": 1, "y": 2}, {"x": 2, "y": 3}] * 5
        write_table("//tmp/t_in", orig_data)

        op = map_reduce(
            in_="//tmp/t_in",
            out=[
                "//tmp/t_out_mapper",
                "//tmp/t_out",
            ],
            reduce_by="x",
            sort_by="x",
            mapper_command="""echo {x=\\"$(date +%Y-%m-%dT%H:%M:%S.%N)\\"} | tee /proc/self/fd/4""",
            # TODO(coteeq): in a number of places, CA assumes, that map job produces chunks of same size
            # mapper_command=(
            #     'for i in $(seq $(($(date +%N) / 50000000))); do'
            #     '    echo "{x=$i;}";'
            #     'done | tee /proc/self/fd/4'
            # ),
            reducer_command=with_breakpoint("cat;BREAKPOINT"),
            spec={
                "sort_locality_timeout": 0,
                "sort_assignment_timeout": 0,
                "enable_partitioned_data_balancing": False,
                "intermediate_data_replication_factor": 1,
                "sort_job_io": {"table_reader": {"retry_count": 1, "pass_count": 1}},
                "mapper_output_table_count": 1,
            },
            track=False,
        )

        first_reduce_job = wait_breakpoint()[0]

        assert self._ban_nodes_with_intermediate_chunks() != []
        self._abort_single_job_if_running_after_node_ban(op, first_reduce_job)

        release_breakpoint()
        op.track()

        assert get(op.get_path() + "/@progress/map_jobs/lost") == 1

        assert_items_equal(read_table("//tmp/t_out_mapper"), read_table("//tmp/t_out"))

    def _remove_intermediate_chunks(self, ratio):
        intermediate_chunk_ids = self._find_intermediate_chunks()
        shuffle(intermediate_chunk_ids)

        removed_chunk_count = 0
        for chunk_id in intermediate_chunk_ids:
            removed = False

            for node in range(self.NUM_NODES):
                chunk_store_path = self.Env.configs["node"][node]["data_node"]["store_locations"][0]["path"]
                chunk_path = os.path.join(chunk_store_path, chunk_id[-2:], chunk_id)
                try:
                    os.remove(chunk_path)
                    os.remove(chunk_path + ".meta")
                    print_debug(f"removed chunk {chunk_id} from node {node} at {chunk_path}")
                    removed = True
                except FileNotFoundError:
                    pass

            if removed:
                removed_chunk_count += 1

            if removed_chunk_count >= ratio * len(intermediate_chunk_ids):
                break

        return removed_chunk_count

    @authors("galtsev")
    @pytest.mark.timeout(600)
    def test_lost_chunks(self):
        jobs_dir = create_tmpdir("jobs")

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"x": x, "y": 1} for x in range(30)])

        job_io = {"table_writer": {"desired_chunk_size": 1}}

        op = map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            reduce_by="x",
            sort_by="x",
            spec={
                "data_size_per_map_job": 1,
                "data_size_per_sort_job": 1,
                "data_size_per_partition_job": 1,
                "data_size_per_sorted_merge_job": 1,
                "data_size_per_reduce_job": 1,
                "map_job_io": job_io,
                "sort_job_io": job_io,
                "reduce_job_io": job_io,
                "try_avoid_duplicating_jobs": True,
            },
            mapper_command=f"""
                date '+%Y-%m-%d %H:%M:%S,%N' | cut -c-26 >> {jobs_dir}/job.$YT_JOB_ID;
                echo mapper >> {jobs_dir}/job.$YT_JOB_ID;
                cat >> {jobs_dir}/job.data.$YT_JOB_ID;
                cat {jobs_dir}/job.data.$YT_JOB_ID;
                cat {jobs_dir}/job.data.$YT_JOB_ID;
                date '+%Y-%m-%d %H:%M:%S,%N' | cut -c-26 >> {jobs_dir}/job.$YT_JOB_ID
            """,
            reduce_combiner_command=f"""
                date '+%Y-%m-%d %H:%M:%S,%N' | cut -c-26 >> {jobs_dir}/job.$YT_JOB_ID;
                echo reduce_combiner >> {jobs_dir}/job.$YT_JOB_ID;
                cat >> {jobs_dir}/job.data.$YT_JOB_ID;
                cat {jobs_dir}/job.data.$YT_JOB_ID;
                cat {jobs_dir}/job.data.$YT_JOB_ID;
                date '+%Y-%m-%d %H:%M:%S,%N' | cut -c-26 >> {jobs_dir}/job.$YT_JOB_ID
            """,
            reducer_command=with_breakpoint(f"""
                BREAKPOINT;
                date '+%Y-%m-%d %H:%M:%S,%N' | cut -c-26 >> {jobs_dir}/job.$YT_JOB_ID;
                echo reducer >> {jobs_dir}/job.$YT_JOB_ID;
                cat >> {jobs_dir}/job.data.$YT_JOB_ID;
                cat {jobs_dir}/job.data.$YT_JOB_ID;
                cat {jobs_dir}/job.data.$YT_JOB_ID;
                date '+%Y-%m-%d %H:%M:%S,%N' | cut -c-26 >> {jobs_dir}/job.$YT_JOB_ID
            """),
            track=False,
        )

        wait_breakpoint()

        removed_chunk_count = self._remove_intermediate_chunks(0.5)
        assert removed_chunk_count > 10
        with Restarter(self.Env, NODES_SERVICE):
            pass

        release_breakpoint()
        op.track()

        assert 8 * get("//tmp/t_in/@row_count") == get("//tmp/t_out/@row_count")


##################################################################


@pytest.mark.opensource
class TestSchedulerMapReduceCommandsMulticell(TestSchedulerMapReduceCommands):
    NUM_SECONDARY_MASTER_CELLS = 2


##################################################################


@pytest.mark.opensource
class TestSchedulerMapReduceCommandsPortal(TestSchedulerMapReduceCommandsMulticell):
    ENABLE_TMP_PORTAL = True


##################################################################


@pytest.mark.opensource
class TestSchedulerMapReduceCommandsNewSortedPool(TestSchedulerMapReduceCommands):
    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "sort_operation_options": {
                "min_uncompressed_block_size": 1
            },
            "map_reduce_operation_options": {
                "min_uncompressed_block_size": 1,
                "job_splitter": {
                    "min_job_time": 3000,
                    "min_total_data_size": 1024,
                    "update_period": 100,
                    "candidate_percentile": 0.8,
                    "max_jobs_per_split": 3,
                    "job_logging_period": 0,
                },
                "spec_template": {
                    "use_new_sorted_pool": True,
                }
            },
            "enable_partition_map_job_size_adjustment": True,
            "operation_options": {
                "spec_template": {
                    "enable_table_index_if_has_trivial_mapper": True,
                },
            },
            "job_tracker": {
                "node_disconnection_timeout": 2000,
            },
        }
    }
