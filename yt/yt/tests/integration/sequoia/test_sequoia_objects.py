from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    NODES_SERVICE
)

from yt_sequoia_helpers import (
    SEQUOIA_CHUNK_TABLES, CHUNK_REPLICAS_TABLE, LOCATION_REPLICAS_TABLE,
    select_rows_from_ground,
)

from yt_commands import (
    authors, create, get, remove, get_singular_chunk_id, write_table, read_table, wait,
    exists, create_domestic_medium, ls, set)
import pytest

##################################################################


def sequoia_tables_empty():
    return all(
        select_rows_from_ground(f"* from [{table.get_path()}]") == []
        for table in SEQUOIA_CHUNK_TABLES)


@pytest.mark.opensource
class TestSequoiaReplicas(YTEnvSetup):
    USE_SEQUOIA = True
    NUM_SECONDARY_MASTER_CELLS = 0
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["sequoia_node_host"]},
    }
    NUM_NODES = 9

    TABLE_MEDIUM = "table_medium"

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "chunk_manager": {
            "sequoia_chunk_replicas_percentage": 100,
            "fetch_replicas_from_sequoia": True
        }
    }

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        node_flavors = [
            ["data", "exec"],
            ["data", "exec"],
            ["data", "exec"],
            ["data", "exec"],
            ["data", "exec"],
            ["data", "exec"],
            ["tablet"],
            ["tablet"],
            ["tablet"],
        ]
        if not hasattr(cls, "node_counter"):
            cls.node_counter = 0
        config["flavors"] = node_flavors[cls.node_counter]
        cls.node_counter = (cls.node_counter + 1) % cls.NUM_NODES

    @classmethod
    def setup_class(cls):
        super(TestSequoiaReplicas, cls).setup_class()
        create_domestic_medium(cls.TABLE_MEDIUM)
        set("//sys/media/{}/@enable_sequoia_replicas".format(cls.TABLE_MEDIUM), True)

        cls.table_node_indexes = []
        addresses_to_index = {cls.Env.get_node_address(index) : index for index in range(0, cls.NUM_NODES)}

        for node_address in ls("//sys/cluster_nodes"):
            flavors = get("//sys/cluster_nodes/{}/@flavors".format(node_address))
            if "data" in flavors:
                location_uuids = list(get("//sys/cluster_nodes/{}/@chunk_locations".format(node_address)).keys())
                assert len(location_uuids) > 0
                for location_uuid in location_uuids:
                    medium_override_path = "//sys/chunk_locations/{}/@medium_override".format(location_uuid)
                    set(medium_override_path, cls.TABLE_MEDIUM)

                cls.table_node_indexes.append(addresses_to_index[node_address])
                if len(cls.table_node_indexes) == 3:
                    break

    def teardown_method(self, method):
        wait(sequoia_tables_empty)
        super(TestSequoiaReplicas, self).teardown_method(method)

    @authors("aleksandra-zh")
    def test_chunk_replicas_node_offline1(self):
        set("//sys/accounts/tmp/@resource_limits/disk_space_per_medium/{}".format(self.TABLE_MEDIUM), 10000)

        create("table", "//tmp/t",  attributes={"primary_medium": self.TABLE_MEDIUM})

        write_table("//tmp/t", [{"x": 1}])
        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")

        assert len(select_rows_from_ground(f"* from [{CHUNK_REPLICAS_TABLE.get_path()}]")) > 0
        wait(lambda: len(select_rows_from_ground(f"* from [{CHUNK_REPLICAS_TABLE.get_path()}]")) == 3)
        wait(lambda: len(select_rows_from_ground(f"* from [{LOCATION_REPLICAS_TABLE.get_path()}]")) == 3)

        with Restarter(self.Env, NODES_SERVICE, indexes=self.table_node_indexes):
            pass

        remove("//tmp/t")

        wait(lambda: not exists("#{}".format(chunk_id)))
        wait(lambda: len(select_rows_from_ground(f"* from [{CHUNK_REPLICAS_TABLE.get_path()}]")) == 0)

    @authors("aleksandra-zh")
    def test_chunk_replicas_node_offline2(self):
        set("//sys/accounts/tmp/@resource_limits/disk_space_per_medium/{}".format(self.TABLE_MEDIUM), 10000)
        create("table", "//tmp/t",  attributes={"primary_medium": self.TABLE_MEDIUM})

        write_table("//tmp/t", [{"x": 1}])
        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")

        assert len(select_rows_from_ground(f"* from [{CHUNK_REPLICAS_TABLE.get_path()}]")) > 0
        wait(lambda: len(select_rows_from_ground(f"* from [{CHUNK_REPLICAS_TABLE.get_path()}]")) == 3)
        wait(lambda: len(select_rows_from_ground(f"* from [{LOCATION_REPLICAS_TABLE.get_path()}]")) == 3)

        with Restarter(self.Env, NODES_SERVICE, indexes=self.table_node_indexes):
            remove("//tmp/t")

            wait(lambda: not exists("#{}".format(chunk_id)))
            wait(lambda: len(select_rows_from_ground(f"* from [{CHUNK_REPLICAS_TABLE.get_path()}]")) == 0)

        wait(lambda: len(select_rows_from_ground(f"* from [{CHUNK_REPLICAS_TABLE.get_path()}]")) == 0)

    @authors("aleksandra-zh")
    def test_replication(self):
        set("//sys/accounts/tmp/@resource_limits/disk_space_per_medium/{}".format(self.TABLE_MEDIUM), 10000)
        create("table", "//tmp/t",  attributes={"primary_medium": self.TABLE_MEDIUM, "replication_factor": 2})

        write_table("//tmp/t", [{"x": 1}], table_writer={"upload_replication_factor": 2})
        wait(lambda: len(select_rows_from_ground(f"* from [{CHUNK_REPLICAS_TABLE.get_path()}]")) == 2)

        assert read_table("//tmp/t") == [{"x": 1}]

        chunk_id = get_singular_chunk_id("//tmp/t")

        set("//tmp/t/@replication_factor", 3)

        wait(lambda: len(get("#{}/@stored_replicas".format(chunk_id))) == 3)
        wait(lambda: len(select_rows_from_ground(f"* from [{CHUNK_REPLICAS_TABLE.get_path()}]")) == 3)

        remove("//tmp/t")


@pytest.mark.opensource
class TestSequoiaReplicasMulticell(TestSequoiaReplicas):
    NUM_SECONDARY_MASTER_CELLS = 3

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        super(TestSequoiaReplicasMulticell, cls).modify_node_config(config, cluster_index)

    @classmethod
    def setup_class(cls):
        super(TestSequoiaReplicasMulticell, cls).setup_class()

    def teardown_method(self, method):
        super(TestSequoiaReplicasMulticell, self).teardown_method(method)
