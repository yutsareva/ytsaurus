from yt_env_setup import YTEnvSetup, wait

# NOTE(asaitgalin): No full yt_commands import here, only rpc api should be used! :)
from yt_commands import discover_proxies, print_debug, authors

from yt_driver_bindings import Driver
from yt_yson_bindings import loads_proto, dumps_proto, loads, dumps

try:
    import yt_proto.yt.client.api.rpc_proxy.proto.api_service_pb2 as api_service_pb2
    import yt_proto.yt.core.misc.proto.error_pb2 as error_pb2

    pb2_imported = True
except ImportError:
    pb2_imported = False

from yt.environment.helpers import assert_items_equal
from yt.common import YtError, underscore_case_to_camel_case
from yt.yson import get_bytes

try:
    from yt.common import uuid_to_parts, parts_to_uuid
except ImportError:
    from yt.common import guid_to_parts as uuid_to_parts, parts_to_guid as parts_to_uuid

from yt.wire_format import (
    AttachmentStream,
    serialize_rows_to_unversioned_wire_format,
    deserialize_rows_from_unversioned_wire_format,
    build_name_table_from_schema,
)

import pytest
import grpc
from datetime import datetime
from copy import deepcopy
from functools import partial
from io import BytesIO

SERIALIZATION_ALIGNMENT = 8


def uuid_from_dict(d):
    return parts_to_uuid(d["first"], d["second"])


def uuid_to_dict(guid):
    parts = uuid_to_parts(guid)
    return {"first": parts[0], "second": parts[1]}


@pytest.mark.skipif(not pb2_imported, reason="Some of pb2 modules could not be imported")
class TestGrpcProxy(YTEnvSetup):
    ENABLE_RPC_PROXY = True
    USE_DYNAMIC_TABLES = True

    @classmethod
    def setup_class(cls):
        super(TestGrpcProxy, cls).setup_class()
        cls.grpc_proxy_address = cls.Env.get_grpc_proxy_address()
        cls.channel = grpc.insecure_channel(cls.grpc_proxy_address)

        config = deepcopy(cls.Env.configs["driver"])
        config["api_version"] = 4
        cls.driver = Driver(config)

    @classmethod
    def teardown_class(cls):
        cls.driver = None
        super(TestGrpcProxy, cls).teardown_class()

    def _wait_response(self, future):
        while True:
            with future._state.condition:
                if future._state.code is not None:
                    break
                future._state.condition.wait(0.1)

    def _make_light_api_request(self, method, params):
        camel_case_method = underscore_case_to_camel_case(method)

        req_msg_class = getattr(api_service_pb2, "TReq" + camel_case_method)
        rsp_msg_class = getattr(api_service_pb2, "TRsp" + camel_case_method)

        unary = self.channel.unary_unary(
            "/ApiService/" + camel_case_method,
            request_serializer=req_msg_class.SerializeToString,
            response_deserializer=rsp_msg_class.FromString,
        )

        metadata = [("yt-protocol-version", "1.0")]

        print_debug()
        print_debug(str(datetime.now()), method, params)

        rsp = unary.future(loads_proto(dumps(params), req_msg_class), metadata=metadata)
        self._wait_response(rsp)
        return dumps_proto(rsp.result())

    def _make_heavy_api_request(self, method, params, data=None, data_serializer=None):
        camel_case_method = underscore_case_to_camel_case(method)

        req_msg_class = getattr(api_service_pb2, "TReq" + camel_case_method)
        rsp_msg_class = getattr(api_service_pb2, "TRsp" + camel_case_method)

        serialized_message = loads_proto(dumps(params), req_msg_class).SerializeToString()
        metadata = [
            ("yt-message-body-size", str(len(serialized_message))),
            ("yt-protocol-version", "1.0"),
        ]

        if data is None:
            to_send = serialized_message
        else:
            stream = BytesIO()
            stream.write(serialized_message)

            if isinstance(data, bytes):
                stream.write(data)
            elif isinstance(data, str):
                stream.write(data.decode("utf-8"))
            else:
                if data_serializer is None:
                    raise YtError("Serializer for structured data should be specified")
                data_serializer(stream, data)

            to_send = stream.getvalue()

        unary = self.channel.unary_unary("/ApiService/" + camel_case_method)
        rsp = unary.future(to_send, metadata=metadata)

        print_debug()
        print_debug(str(datetime.now()), method, params)

        self._wait_response(rsp)

        message_body_size = None
        for metadata in rsp.trailing_metadata():
            if metadata[0] == "yt-message-body-size":
                message_body_size = int(metadata[1])
                break

        result = rsp.result()

        if message_body_size is None:
            message_body_size = len(result)

        rsp_msg = rsp_msg_class.FromString(result[:message_body_size])
        return loads(dumps_proto(rsp_msg)), AttachmentStream(result, message_body_size)

    def _get_node(self, **kwargs):
        rsp = loads(self._make_light_api_request("get_node", kwargs))
        return loads(get_bytes(rsp["value"]))

    def _create_node(self, **kwargs):
        return loads(self._make_light_api_request("create_node", kwargs))["node_id"]

    def _create_object(self, **kwargs):
        object_id_parts = loads(self._make_light_api_request("create_object", kwargs))["object_id"]
        return uuid_from_dict(object_id_parts)

    def _exists_node(self, **kwargs):
        return loads(self._make_light_api_request("exists_node", kwargs))["exists"]

    def _mount_table(self, **kwargs):
        self._make_light_api_request("mount_table", kwargs)

    def _start_transaction(self, **kwargs):
        id_parts = loads(self._make_light_api_request("start_transaction", kwargs))["id"]
        return uuid_from_dict(id_parts)

    def _commit_transaction(self, **kwargs):
        return loads(self._make_light_api_request("commit_transaction", kwargs))

    @authors("asaitgalin")
    def test_cypress_commands(self):
        # 700 = "map_node", see ytlib/object_client/public.h
        self._create_node(type=303, path="//tmp/test")
        assert self._exists_node(path="//tmp/test")
        assert self._get_node(path="//tmp/test/@type") == "map_node"

    def _sync_create_cell(self):
        # 700 = "tablet_cell", see ytlib/object_client/public.h
        cell_id = self._create_object(type=700)
        print_debug("Waiting for tablet cell", cell_id, "to become healthy...")

        def check_cell():
            cell = self._get_node(
                path="//sys/tablet_cells/" + cell_id,
                attributes={"keys": ["id", "health", "peers"]},
            )
            if cell.attributes["health"] != "good":
                return False

            node = cell.attributes["peers"][0]["address"]
            if not self._exists_node(
                path="//sys/cluster_nodes/{0}/orchid/tablet_cells/{1}".format(node, cell.attributes["id"])
            ):
                return False

            return True

        wait(check_cell)

    def _sync_mount_table(self, path):
        self._mount_table(path=path)
        wait(lambda: all(tablet["state"] == "mounted" for tablet in self._get_node(path=path + "/@tablets")))

    @authors("asaitgalin")
    def test_dynamic_table_commands(self):
        self._sync_create_cell()

        table_path = "//tmp/test"

        schema = [
            {"name": "a", "type": "string", "sort_order": "ascending"},
            {"name": "b", "type": "int64"},
            {"name": "c", "type": "uint64"},
        ]

        # 401 = "table", see ytlib/object_client/public.h
        self._create_node(type=401, path=table_path, attributes={"dynamic": True, "schema": schema})
        self._sync_mount_table(table_path)
        # 1 = "tablet", see ETransactionType in proto
        tx = self._start_transaction(type=1, timeout=10000000, sticky=True)
        print_debug(tx)

        rows = [
            {"a": "Look", "b": 1},
            {"a": "Morty", "b": 2},
            {"a": "I", "c": 3},
            {"a": "am", "b": 7},
            {"a": "pickle", "c": 4},
            {"a": "Rick!", "b": 3},
        ]

        self._make_heavy_api_request(
            "modify_rows",
            {
                "transaction_id": uuid_to_dict(tx),
                "path": table_path,
                # 0 = "write", see ERowModificationType in proto
                "row_modification_types": [0] * len(rows),
                "rowset_descriptor": {"name_table_entries": build_name_table_from_schema(schema)},
            },
            data=rows,
            data_serializer=partial(serialize_rows_to_unversioned_wire_format, schema=schema),
        )

        self._commit_transaction(transaction_id=uuid_to_dict(tx))

        msg, stream = self._make_heavy_api_request("select_rows", {"query": "* FROM [{}]".format(table_path)})
        selected_rows = deserialize_rows_from_unversioned_wire_format(
            stream,
            [entry["name"] for entry in msg["rowset_descriptor"]["name_table_entries"]],
        )

        assert_items_equal(selected_rows, rows)

    @authors("asaitgalin")
    def test_protocol_version(self):
        msg = api_service_pb2.TReqGetNode(path="//tmp")

        unary = self.channel.unary_unary(
            "/ApiService/GetNode",
            request_serializer=api_service_pb2.TReqGetNode.SerializeToString,
            response_deserializer=api_service_pb2.TReqGetNode.FromString,
        )

        # Require min protocol version
        rsp = unary.future(msg, metadata=[("yt-protocol-version", "3.14")])
        self._wait_response(rsp)

        error_found = False
        for key, value in rsp.trailing_metadata():
            if key == "yt-error-bin":
                error = error_pb2.TError()
                error.ParseFromString(value)
                assert "protocol version" in error.message
                error_found = True
                break

        assert error_found, "Request should fail!"

    @authors("kiselyovp")
    def test_discovery(self):
        proxies = discover_proxies(type_="grpc", driver=self.driver)
        assert len(proxies) == self.NUM_RPC_PROXIES
        assert self.grpc_proxy_address in proxies
