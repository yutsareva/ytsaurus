from .proxy_format_config import _TestProxyFormatConfigBase

from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE, NODES_SERVICE, is_asan_build

from yt_helpers import profiler_factory, read_structured_log, write_log_barrier

from yt_commands import (
    authors, wait, create, ls, get, set, remove, map,
    create_user, create_proxy_role, issue_token, make_ace,
    create_access_control_object_namespace, create_access_control_object,
    with_breakpoint, wait_breakpoint,
    read_table, write_table, Operation)

from yt.common import YtResponseError
import yt.packages.requests as requests
import yt.yson as yson
import yt_error_codes

import pytest

import collections
import json
import os
import struct
import socket
from datetime import datetime, timedelta
import time


def try_parse_yt_error_headers(rsp):
    if "X-YT-Error" in rsp.headers:
        assert "X-YT-Framing" not in rsp.headers
        raise YtResponseError(json.loads(rsp.headers.get("X-YT-Error")))
    rsp.raise_for_status()


def try_parse_yt_error_trailers(rsp):
    trailers = rsp.trailers()
    if trailers is not None and "X-YT-Error" in trailers:
        assert "X-YT-Framing" not in trailers
        raise YtResponseError(json.loads(trailers.get("X-YT-Error")))

##################################################################


class HttpProxyTestBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_HTTP_PROXIES = 1
    NUM_RPC_PROXIES = 2

    DELTA_PROXY_CONFIG = {
        "coordinator": {
            "heartbeat_interval": 100,
            "death_age": 500,
            "cypress_timeout": 50,
        },
        "api": {
            "force_tracing": True,
        },
        "access_checker": {
            "enabled": True,
            "cache": {
                "expire_after_access_time": 100,
            },
        },
    }

    def _get_proxy_address(self):
        return "http://" + self.Env.get_proxy_address()

    def _get_build_snapshot_url(self):
        return self._get_proxy_address() + "/api/v4/build_snapshot"

    def _get_master_exit_read_only_url(self):
        return self._get_proxy_address() + "/api/v4/master_exit_read_only"

    def _get_master_address(self):
        return ls("//sys/primary_masters", suppress_transaction_coordinator_sync=True)[0]

    def _get_hydra_monitoring(self, master=None):
        if master is None:
            master = self._get_master_address()
        return get(
            "//sys/primary_masters/{}/orchid/monitoring/hydra".format(master),
            suppress_transaction_coordinator_sync=True,
            default={},
        )


@pytest.mark.opensource
class TestHttpProxy(HttpProxyTestBase):
    def teardown_method(self, method):
        for proxy in ls("//sys/http_proxies"):
            set("//sys/http_proxies/{}/@role".format(proxy), "data")
        super(TestHttpProxy, self).teardown_method(method)

    @authors("prime")
    def test_ping(self):
        rsp = requests.get(self._get_proxy_address() + "/ping")
        rsp.raise_for_status()

    @authors("prime")
    def test_version(self):
        rsp = requests.get(self._get_proxy_address() + "/version")
        rsp.raise_for_status()

    @authors("prime")
    def test_service(self):
        service = requests.get(self._get_proxy_address() + "/service").json()
        assert "version" in service
        assert "start_time" in service

    @authors("levysotsky")
    def test_hosts(self):
        proxy = ls("//sys/http_proxies")[0]

        def get_yson(url):
            return yson.loads(requests.get(url).content)

        assert get_yson(self._get_proxy_address() + "/hosts") == [proxy]
        assert get_yson(self._get_proxy_address() + "/hosts?role=data") == [proxy]
        assert get_yson(self._get_proxy_address() + "/hosts?role=control") == []

        def make_failing_request_and_check_counter(counter):
            url = self._get_proxy_address() + "/api/v3/find_meaning_of_life"
            requests.get(url)
            return counter.get_delta() > 0

        profiler = profiler_factory().at_http_proxy(proxy, fixed_tags={"http_code": "404"})
        data_http_code_counter = profiler.counter("http_proxy/http_code_count", tags={"proxy_role": "data"})

        wait(lambda: make_failing_request_and_check_counter(data_http_code_counter))

        set("//sys/http_proxies/" + proxy + "/@role", "control")

        def check_role_updated():
            return get_yson(self._get_proxy_address() + "/hosts") == [] and \
                get_yson(self._get_proxy_address() + "/hosts?role=data") == [] and \
                get_yson(self._get_proxy_address() + "/hosts?role=control") == [proxy]

        # Wait until the proxy entry will be updated on the coordinator.
        wait(check_role_updated)

        control_http_code_counter = profiler.counter("http_proxy/http_code_count", tags={"proxy_role": "control"})

        wait(lambda: make_failing_request_and_check_counter(control_http_code_counter))

        hosts = requests.get(self._get_proxy_address() + "/hosts/all").json()
        assert len(hosts) == 1
        assert not hosts[0]["banned"]

    @authors("prime")
    def test_supported_api_versions(self):
        assert ["v3", "v4"] == requests.get(self._get_proxy_address() + "/api").json()

    @authors("prime")
    def test_discover_versions_v2(self):
        rsp = requests.get(self._get_proxy_address() + "/internal/discover_versions/v2")
        rsp.raise_for_status()

        versions = rsp.json()
        assert "details" in versions
        assert "summary" in versions

        counts = collections.Counter()

        for instance in versions["details"]:
            assert "address" in instance
            assert "start_time" in instance
            assert "type" in instance
            assert "version" in instance

            counts[instance["type"]] += 1

        assert counts["primary_master"] == 1
        assert counts["secondary_master"] == 2
        assert counts["cluster_node"] == 5
        assert counts["scheduler"] == 1
        assert counts["controller_agent"] == 1
        assert counts["http_proxy"] == 1
        assert counts["rpc_proxy"] == 2

    @authors("prime")
    def test_cache_control(self):
        rsp = requests.get(self._get_proxy_address() + "/api/v4/get?path=//@")
        rsp.raise_for_status()

        assert rsp.headers["cache-control"] == "no-store"

    @authors("prime")
    def test_dynamic_config(self):
        monitoring_port = self.Env.configs["http_proxy"][0]["monitoring_port"]
        config_url = "http://localhost:{}/orchid/dynamic_config_manager/effective_config".format(monitoring_port)

        set("//sys/http_proxies/@config", {"tracing": {"user_sample_rate": {"prime": 1.0}}})

        def config_updated():
            config = requests.get(config_url).json()
            return "prime" in config["tracing"]["user_sample_rate"]

        wait(config_updated)

    @authors("prime")
    def test_taken_port(self):
        pytest.skip()

        monitoring_port = self.Env.configs["node"][0]["monitoring_port"]

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        with Restarter(self.Env, NODES_SERVICE):
            s.bind(("127.0.0.1", monitoring_port))
            s.listen(1)

    @authors("greatkorn")
    def test_kill_nodes(self):
        create("map_node", "//sys/http_proxies/test_http_proxy")
        set(
            "//sys/http_proxies/test_http_proxy/@liveness",
            {"updated_at": "2010-06-24T11:23:30.156098Z"},
        )
        set("//sys/http_proxies/test_http_proxy/@start_time", "2009-06-19T16:39:02.171721Z")
        set("//sys/http_proxies/test_http_proxy/@version", "19.5.30948-master-ya~c9facaeaca")
        create("map_node", "//sys/rpc_proxies/test_rpc_proxy")
        set(
            "//sys/rpc_proxies/test_rpc_proxy/@start_time",
            "2009-06-19T16:39:02.171721Z",
        )
        set(
            "//sys/rpc_proxies/test_rpc_proxy/@version",
            "19.5.30948-master-ya~c9facaeaca",
        )

        rsp = requests.get(self._get_proxy_address() + "/internal/discover_versions/v2")
        rsp.raise_for_status()

        status = rsp.json()
        for proxy in status["details"]:
            if proxy["address"] in ("test_http_proxy", "test_rpc_proxy"):
                assert proxy.get("state") == "offline"

        remove("//sys/http_proxies/test_http_proxy")
        remove("//sys/rpc_proxies/test_rpc_proxy")

    @authors("greatkorn")
    def test_structured_logs(self):
        proxy_address = "localhost:" + str(self.Env.configs["http_proxy"][0]["rpc_port"])

        from_barrier = write_log_barrier(proxy_address)
        client = self.Env.create_client()
        client.list("//sys")
        to_barrier = write_log_barrier(proxy_address)

        log_path = self.path_to_run + "/logs/http-proxy-0.json.log"
        events = read_structured_log(log_path, from_barrier, to_barrier)

        has_log_entry = False
        for event in events:
            if event.get("path") == "//sys":
                has_log_entry = True
        assert has_log_entry

    @authors("greatkorn")
    def test_fail_logging(self):
        requests.get(self._get_proxy_address() + "/api/v2/get")

    @authors("alexkolodezny")
    def test_banned_proxy(self):
        proxy = ls("//sys/http_proxies")[0]
        set("//sys/http_proxies/" + proxy + "/@banned", True)
        wait(lambda: not requests.get(self._get_proxy_address() + "/ping").ok)

        set("//sys/http_proxies/" + proxy + "/@banned", False)
        wait(lambda: requests.get(self._get_proxy_address() + "/ping").ok)

    @authors("alexkolodezny")
    def test_proxy_unavailable_on_master_failure(self):
        with Restarter(self.Env, MASTERS_SERVICE):
            wait(lambda: not requests.get(self._get_proxy_address() + "/ping").ok)

        wait(lambda: requests.get(self._get_proxy_address() + "/ping").ok)


class HttpProxyAccessCheckerTestBase(HttpProxyTestBase):
    DELTA_PROXY_CONFIG = {
        "access_checker": {
            "enabled": True,
            "cache": {
                "expire_after_access_time": 100,
            },
        },
    }

    @authors("gritukan", "verytable")
    def test_access_checker(self):
        def check_access(proxy_address, user):
            url = "{}/api/v3/get?path=//sys/@config".format(proxy_address)
            rsp = requests.get(url, headers={"X-YT-User-Name": user})
            assert rsp.status_code == 200 or rsp.status_code == 403
            return rsp.status_code == 200

        self.create_proxy_role_namespace()

        create_user("u")
        self.create_proxy_role("r1")
        self.create_proxy_role("r2")

        self.set_acl("r1", [make_ace("deny", "u", "use")])
        self.set_acl("r2", [make_ace("allow", "u", "use")])

        proxy = ls("//sys/http_proxies")[0]
        proxy_address = self._get_proxy_address()

        # "u" is not allowed to use proxies with role "r1".
        set("//sys/http_proxies/" + proxy + "/@role", "r1")
        wait(lambda: not check_access(proxy_address, "u"))

        # "u" is allowed to use proxies with role "r2".
        set("//sys/http_proxies/" + proxy + "/@role", "r2")
        wait(lambda: check_access(proxy_address, "u"))

        # Now "u" is not allowed to use proxies with role "r2".
        self.set_acl("r2", [make_ace("deny", "u", "use")])
        wait(lambda: not check_access(proxy_address, "u"))

        # There is no node for proxy role "r3". By default we allow access to
        # proxies with unknown role.
        set("//sys/http_proxies/" + proxy + "/@role", "r3")
        wait(lambda: check_access(proxy_address, "u"))

        # Set proxy role back to "r2". User "u" still can't use it.
        set("//sys/http_proxies/" + proxy + "/@role", "r2")
        wait(lambda: not check_access(proxy_address, "u"))

        # Disable access checker via dynamic config. Now "u" can use proxy.
        set("//sys/http_proxies/@config", {"access_checker": {"enabled": False}})
        wait(lambda: check_access(proxy_address, "u"))

        # Enable access checker via dynamic config. And "u" is banned again.
        set("//sys/http_proxies/@config", {"access_checker": {"enabled": True}})
        wait(lambda: not check_access(proxy_address, "u"))


@pytest.mark.opensource
class TestHttpProxyAccessChecker(HttpProxyAccessCheckerTestBase):
    def create_proxy_role_namespace(self):
        create("http_proxy_role_map", "//sys/http_proxy_roles")

    def create_proxy_role(self, name):
        create_proxy_role(name, "http")

    def set_acl(self, role, acl):
        set(f"//sys/http_proxy_roles/{role}/@acl", acl)


@pytest.mark.opensource
class TestHttpProxyAccessCheckerWithAco(HttpProxyAccessCheckerTestBase):
    @classmethod
    def setup_class(cls):
        cls.DELTA_PROXY_CONFIG["access_checker"].update({
            "use_access_control_objects": True,
            "path_prefix": "//sys/access_control_object_namespaces/http_proxy_roles",
        })
        super().setup_class()

    def create_proxy_role_namespace(self):
        create_access_control_object_namespace("http_proxy_roles")

    def create_proxy_role(self, name):
        create_access_control_object(name, "http_proxy_roles")

    def set_acl(self, role, acl):
        set(f"//sys/access_control_object_namespaces/http_proxy_roles/{role}/principal/@acl", acl)


@pytest.mark.opensource
class TestHttpProxyRoleFromStaticConfig(HttpProxyTestBase):
    DELTA_PROXY_CONFIG = {
        "role": "ab"
    }

    @authors("nadya73")
    def test_role(self):
        proxy = ls("//sys/http_proxies")[0]
        role = get("//sys/http_proxies/" + proxy + "/@role")
        assert role == "ab"

    @authors("nadya73")
    def test_hosts(self):
        proxy = ls("//sys/http_proxies")[0]

        def get_yson(url):
            return yson.loads(requests.get(url).content)

        assert get_yson(self._get_proxy_address() + "/hosts") == []
        assert get_yson(self._get_proxy_address() + "/hosts?role=data") == []
        assert get_yson(self._get_proxy_address() + "/hosts?role=ab") == [proxy]


@pytest.mark.opensource
class TestHttpProxyAuth(HttpProxyTestBase):
    @classmethod
    def setup_class(cls):
        cls.DELTA_PROXY_CONFIG["auth"] = {
            "enable_authentication": True,
        }
        super(TestHttpProxyAuth, cls).setup_class()

    @authors("mpereskokova")
    def test_access_on_behalf_of_the_user(self):
        proxy_address = self._get_proxy_address()

        def create_user_with_token(user):
            create_user(user)
            token, _ = issue_token(user)
            return token

        def check_access(proxy_address, path="/", status_code=200, error_code=None, user=None, token=None):
            url = "{}/api/v4/get?path={}".format(proxy_address, path)
            headers = {}
            if user:
                headers["X-YT-User-Name"] = user
            if token:
                headers["Authorization"] = "OAuth " + token
            rsp = requests.get(url, headers=headers)

            if error_code is not None :
                assert json.loads(rsp.content)["code"] == error_code

            assert rsp.status_code in [200, 400, 401]
            return rsp.status_code == status_code

        yql_agent_token = create_user_with_token("yql_agent")
        test_user_token = create_user_with_token("test_user")

        wait(lambda: check_access(proxy_address, status_code=200, token=yql_agent_token))
        wait(lambda: check_access(proxy_address, status_code=200, token=yql_agent_token, user="test_user"))
        wait(lambda: check_access(proxy_address, status_code=401, token=test_user_token, user="yql_agent"))

        node = "//tmp/dir"
        create(
            "map_node",
            node,
            attributes={"acl": [
                {"action": "deny", "subjects": ["yql_agent"], "permissions": ["read"]},
                {"action": "allow", "subjects": ["test_user"], "permissions": ["read"]},
            ]},
        )
        wait(lambda: check_access(proxy_address, path=node, status_code=400, error_code=yt_error_codes.AuthorizationErrorCode, token=yql_agent_token))
        wait(lambda: check_access(proxy_address, path=node, status_code=200, token=yql_agent_token, user="test_user"))


@pytest.mark.opensource
class TestHttpProxyFraming(HttpProxyTestBase):
    SUSPENDING_TABLE = "//tmp/suspending_table"
    DELAY_BEFORE_COMMAND = 10 * 1000
    KEEP_ALIVE_PERIOD = 0.1 * 1000
    # CLIENT_TIMEOUT << DELAY_BEFORE_COMMAND to catch framing bugs
    # CLIENT_TIMEOUT >> KEEP_ALIVE_PERIOD to avoid false test failures
    CLIENT_TIMEOUT = 1 * 1000
    DELTA_PROXY_CONFIG = {
        "api": {
            "testing": {
                "delay_before_command": {
                    "get": {
                        "delay": DELAY_BEFORE_COMMAND,
                        "parameter_path": "/path",
                        "substring": SUSPENDING_TABLE,
                    },
                    "get_table_columnar_statistics": {
                        "delay": DELAY_BEFORE_COMMAND,
                        "parameter_path": "/paths/0",
                        "substring": SUSPENDING_TABLE,
                    },
                    "partition_tables": {
                        "delay": DELAY_BEFORE_COMMAND,
                        "parameter_path": "/paths/0",
                        "substring": SUSPENDING_TABLE,
                    },
                    "read_table": {
                        "delay": DELAY_BEFORE_COMMAND,
                        "parameter_path": "/path",
                        "substring": SUSPENDING_TABLE,
                    },
                }
            },
        },
    }

    FRAME_TAG_TO_NAME = {
        0x01: "data",
        0x02: "keep_alive",
    }

    @classmethod
    def _unframe_content(cls, content):
        result = []
        i = 0
        while i < len(content):
            tag = content[i]
            i += 1
            assert tag in cls.FRAME_TAG_TO_NAME
            name = cls.FRAME_TAG_TO_NAME[tag]
            if name == "data":
                (length,) = struct.unpack("<i", content[i:i + 4])
                i += 4
                assert i + length <= len(content)
                frame = content[i:i + length]
                i += length
            else:
                frame = None
            result.append((name, frame))
        return result

    def setup_method(self, method):
        super(TestHttpProxyFraming, self).setup_method(method)
        monitoring_port = self.Env.configs["http_proxy"][0]["monitoring_port"]
        config_url = "http://localhost:{}/orchid/dynamic_config_manager/effective_config".format(monitoring_port)
        set(
            "//sys/http_proxies/@config",
            {"framing": {"keep_alive_period": self.KEEP_ALIVE_PERIOD}},
        )
        wait(lambda: requests.get(config_url).json()["framing"]["keep_alive_period"] == self.KEEP_ALIVE_PERIOD)

    def _execute_command(self, http_method, command_name, params, extra_headers={}):
        headers = {
            "X-YT-Accept-Framing": "1",
            "X-YT-Parameters": yson.dumps(params),
            "X-YT-Header-Format": "<format=text>yson",
            "X-YT-Output-Format": "<format=text>yson",
        }
        headers.update(extra_headers)
        start = datetime.now()
        rsp = requests.request(
            http_method,
            "{}/api/v4/{}".format(self._get_proxy_address(), command_name),
            headers=headers,
            timeout=timedelta(milliseconds=self.CLIENT_TIMEOUT).total_seconds(),
        )
        try_parse_yt_error_headers(rsp)

        assert "X-YT-Framing" in rsp.headers
        unframed_content = self._unframe_content(rsp.content)
        try_parse_yt_error_trailers(rsp)

        keep_alive_frame_count = sum(name == "keep_alive" for name, frame in unframed_content)
        assert keep_alive_frame_count >= self.DELAY_BEFORE_COMMAND / self.KEEP_ALIVE_PERIOD - 3
        assert datetime.now() - start > timedelta(milliseconds=self.DELAY_BEFORE_COMMAND)
        actual_response = b""
        for name, frame in unframed_content:
            if name == "data":
                actual_response += frame
        return actual_response

    @authors("levysotsky")
    def test_get(self):
        create("table", self.SUSPENDING_TABLE)
        write_table(self.SUSPENDING_TABLE, [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])
        attribute_value = {"x": 1, "y": "qux"}
        set(self.SUSPENDING_TABLE + "/@foobar", attribute_value)
        params = {
            "path": self.SUSPENDING_TABLE + "/@foobar",
        }
        response = self._execute_command("GET", "get", params)
        response_yson = yson.loads(response)
        assert {"value": attribute_value} == response_yson

    @authors("levysotsky")
    @pytest.mark.parametrize("use_compression", [True, False])
    def test_read_table(self, use_compression):
        create("table", self.SUSPENDING_TABLE)
        rows_chunk = [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}] * 100
        chunk_count = 100
        for _ in range(chunk_count):
            write_table("<append=%true>" + self.SUSPENDING_TABLE, rows_chunk)
        params = {
            "path": self.SUSPENDING_TABLE,
            "output_format": "yson",
        }
        headers = {}
        if not use_compression:
            headers["Accept-Encoding"] = "identity"
        response = self._execute_command("GET", "read_table", params, extra_headers=headers)
        response_yson = list(yson.loads(response, yson_type="list_fragment"))
        assert rows_chunk * chunk_count == response_yson

        params = {
            "path": self.SUSPENDING_TABLE + "-nonexistent",
            "output_format": "yson",
        }
        with pytest.raises(YtResponseError):
            self._execute_command("GET", "read_table", params, extra_headers=headers)

    @authors("levysotsky", "denvid")
    def test_get_table_columnar_statistics(self):
        create("table", self.SUSPENDING_TABLE)
        write_table(self.SUSPENDING_TABLE, [{"column_1": 1, "column_2": "foo"}])
        params = {
            "paths": [self.SUSPENDING_TABLE + "{column_1}"],
        }
        response = self._execute_command("GET", "get_table_columnar_statistics", params)
        statistics = yson.loads(response)
        assert len(statistics) == 1
        assert statistics[0].get("column_data_weights") == {"column_1": 8}
        assert statistics[0].get("column_min_values") == {"column_1": 1}
        assert statistics[0].get("column_max_values") == {"column_1": 1}
        assert statistics[0].get("column_non_null_value_counts") == {"column_1": 1}
        assert statistics[0].get("chunk_row_count") == 1
        assert statistics[0].get("legacy_chunk_row_count") == 0

        remove(self.SUSPENDING_TABLE)
        with pytest.raises(YtResponseError):
            self._execute_command("GET", "get_table_columnar_statistics", params)

    @authors("galtsev")
    @pytest.mark.parametrize("encoding", ["identity", "gzip", "deflate", "brotly"])
    def test_partition_tables(self, encoding):
        create("table", self.SUSPENDING_TABLE)
        write_table(self.SUSPENDING_TABLE, [{"column_1": 1, "column_2": "foo"}])
        params = {
            "paths": [self.SUSPENDING_TABLE + "{column_1}"],
            "data_weight_per_partition": 1 << 30,
        }
        headers = {
            "Accept-Encoding": encoding,
        }
        response = self._execute_command("GET", "partition_tables", params, extra_headers=headers)
        partitioning_response = yson.loads(response)
        assert len(partitioning_response) == 1
        assert "partitions" in partitioning_response
        assert "table_ranges" in partitioning_response["partitions"][0]

        remove(self.SUSPENDING_TABLE)
        with pytest.raises(YtResponseError):
            self._execute_command("GET", "get_table_columnar_statistics", params, extra_headers=headers)


@pytest.mark.opensource
class TestHttpProxyJobShellAudit(HttpProxyTestBase):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "test_poll_job_shell": True,
            },
        },
    }

    USE_PORTO = True

    @classmethod
    def modify_proxy_config(cls, configs):
        for i in range(len(configs)):
            configs[i]["logging"]["flush_period"] = 100
            configs[i]["logging"]["rules"].append(
                {
                    "min_level": "info",
                    "writers": ["job_shell"],
                    "include_categories": ["JobShell"],
                    "message_format": "structured",
                }
            )
            configs[i]["logging"]["writers"]["job_shell"] = {
                "type": "file",
                "file_name": os.path.join(cls.path_to_run, "logs/job-shell-{}.json.log".format(i)),
                "accepted_message_format": "structured",
            }

    @authors("psushin")
    def test_job_shell_logging(self):
        create("table", "//tmp/t_in", force=True)
        write_table("//tmp/t_in", [{"a": "b"}])
        op = map(
            track=False,
            label="poll_job_shell",
            in_="//tmp/t_in",
            command=with_breakpoint("cat > /dev/null; BREAKPOINT"))

        jobs = wait_breakpoint()

        headers = {
            "X-YT-Parameters": yson.dumps({
                "job_id": jobs[0],
                "parameters": {
                    "operation": "spawn",
                    "term": "screen-256color",
                    "height": 50,
                    "width": 132,
                },
            }),
            "X-YT-Header-Format": "<format=text>yson",
            "X-YT-Output-Format": "<format=text>yson",
        }

        rsp = requests.request(
            "post",
            "{}/api/v4/{}".format(self._get_proxy_address(), "poll_job_shell"),
            headers=headers
        )
        try_parse_yt_error_headers(rsp)

        result = yson.loads(rsp.content)
        shell_id = result["result"]["shell_id"]

        path = os.path.join(self.path_to_run, "logs/job-shell-0.json.log")

        def has_shell_start_record():
            if not os.path.exists(path):
                return False
            with open(path) as f:
                for line_json in f.readlines():
                    if json.loads(line_json.strip())["shell_id"] == shell_id:
                        return True
            return False

        wait(lambda: has_shell_start_record(), iter=120, sleep_backoff=1.0)

        op.abort()


@pytest.mark.opensource
class TestHttpProxyFormatConfig(HttpProxyTestBase, _TestProxyFormatConfigBase):
    NUM_TEST_PARTITIONS = 6

    def setup_method(self, method):
        super(TestHttpProxyFormatConfig, self).setup_method(method)
        monitoring_port = self.Env.configs["http_proxy"][0]["monitoring_port"]
        config_url = "http://localhost:{}/orchid/dynamic_config_manager/effective_config".format(monitoring_port)
        set("//sys/http_proxies/@config", {"formats": self.FORMAT_CONFIG})

        def config_updated():
            config = requests.get(config_url).json()
            return config \
                .get("formats", {}) \
                .get("yamred_dsv", {}) \
                .get("user_overrides", {}) \
                .get("good_user", False)

        wait(config_updated)

    def _execute_command(self, http_method, command_name, params, user="root", data=None,
                         header_format="yson", input_format="yson", output_format="yson",
                         api_version="v4"):
        headers = {
            "X-YT-Parameters": self._write_format(header_format, params, tabular=False),
            "X-YT-Header-Format": self._write_format("yson", header_format, tabular=False),
            "X-YT-Output-Format": self._write_format(header_format, output_format, tabular=False),
            "X-YT-Input-Format": self._write_format(header_format, input_format, tabular=False),
            "X-YT-User-Name": user,
        }
        rsp = requests.request(
            http_method,
            "{address}/api/{api_version}/{command_name}".format(
                address=self._get_proxy_address(),
                api_version=api_version,
                command_name=command_name,
            ),
            headers=headers,
            data=data,
        )
        try_parse_yt_error_headers(rsp)
        return rsp

    def _do_run_operation(self, op_type, spec, user, use_start_op):
        params = {"spec": spec, "operation_type": op_type}
        command = "start_operation" if use_start_op else op_type
        api_version = "v4" if use_start_op else "v3"
        rsp = self._execute_command(
            "post",
            command,
            params,
            user=user,
            api_version=api_version,
        )
        result = yson.loads(rsp.content)
        op = Operation()
        op.id = result if api_version == "v3" else result["operation_id"]
        return op

    def _test_format_enable_general(self, format, user, enable):
        create("table", "//tmp/t", force=True)
        content = self.TABLE_CONTENT_TWO_COLUMNS
        write_table("//tmp/t", content)

        manager = self._get_context_manager(enable)

        with manager():
            rsp = self._execute_command(
                "get",
                "read_table",
                {"path": "//tmp/t"},
                user=user,
                output_format=format,
            )
            assert self._parse_format(format, rsp.content) == content

        with manager():
            rsp = self._execute_command(
                "get",
                "read_table",
                {"path": "//tmp/t", "output_format": format},
                user=user,
            )
            assert self._parse_format(format, rsp.content) == content

        with manager():
            self._execute_command(
                "put",
                "write_table",
                {"path": "//tmp/t"},
                user=user,
                input_format=format,
                data=self._write_format(format, content),
            )
            assert read_table("//tmp/t") == content

        with manager():
            self._execute_command(
                "put",
                "write_table",
                {"path": "//tmp/t", "input_format": format},
                user=user,
                data=self._write_format(format, content),
            )
            assert read_table("//tmp/t") == content

    def _test_format_enable(self, format, user, enable):
        self._test_format_enable_general(format, user, enable)
        self._test_format_enable_operations(format, user, enable)

    @authors("levysotsky")
    def test_header_format_enable(self):
        create_user("no_json_user")
        manager = self._get_context_manager(enable=False)
        with manager():
            self._execute_command(
                "get",
                "get",
                {"path": "//tmp/t/@type"},
                user="no_json_user",
                header_format="json",
                output_format=self.YSON,
            )

    def _test_format_defaults_cypress(self, format, user, content, expected_content):
        set("//tmp/list_node", content, force=True)

        rsp = self._execute_command(
            "GET",
            "get",
            {"path": "//tmp/list_node"},
            user=user,
            output_format=format,
        )
        actual_content = rsp.content
        assert actual_content == expected_content

        rsp = self._execute_command(
            "GET",
            "get",
            {"path": "//tmp/list_node", "output_format": format},
            user=user,
        )
        actual_content = rsp.content
        assert actual_content == expected_content

    def _test_format_defaults(self, format, user, content, expected_format):
        assert str(expected_format) == "yson"
        yson_format = expected_format.attributes.get("format", "text")
        expected_content_cypress = yson.dumps({"value": content}, yson_format=yson_format)
        expected_content_operation = yson.dumps(content, yson_format=yson_format, yson_type="list_fragment")
        self._test_format_defaults_cypress(format, user, content, expected_content_cypress)
        self._test_format_defaults_operations(format, user, content, expected_content_operation)


@pytest.mark.opensource
class TestHttpProxyBuildSnapshotBase(HttpProxyTestBase):
    NUM_SCHEDULERS = 0

    DELTA_MASTER_CONFIG = {
        "hydra_manager": {
            "build_snapshot_delay": 10000,
        },
    }

    DELTA_PROXY_CONFIG = {
        "coordinator": {
            "cypress_timeout": 50,
        },
    }

    def _build_snapshot(self, set_read_only):
        params = {
            "cell_id": self.Env.configs["master"][0]["primary_master"]["cell_id"],
            "set_read_only": set_read_only,
            "wait_for_snapshot_completion": False,
        }
        headers = {
            "X-YT-Parameters": yson.dumps(params),
            "X-YT-Header-Format": "<format=text>yson",
            "X-YT-Output-Format": "<format=text>yson",
        }

        rsp = requests.post(self._get_build_snapshot_url(), headers=headers)
        rsp.raise_for_status()

        return yson.loads(rsp.content)["snapshot_id"]

    def _master_exit_read_only(self):
        params = {
            "retry": True,
        }
        headers = {
            "X-YT-Parameters": yson.dumps(params),
            "X-YT-Header-Format": "<format=text>yson",
            "X-YT-Output-Format": "<format=text>yson",
        }

        rsp = requests.post(self._get_master_exit_read_only_url(), headers=headers)
        rsp.raise_for_status()

    def _build_snapshot_and_check(self, set_read_only):
        master = self._get_master_address()

        def _check_not_building_snapshot():
            monitoring = self._get_hydra_monitoring(master)
            return "building_snapshot" in monitoring and not monitoring["building_snapshot"]
        wait(_check_not_building_snapshot)

        snapshot_id = self._build_snapshot(set_read_only)

        def _check_building_snapshot():
            monitoring = self._get_hydra_monitoring(master)
            return "building_snapshot" in monitoring and monitoring["building_snapshot"]
        wait(_check_building_snapshot)

        def _check_snapshot_built():
            monitoring = self._get_hydra_monitoring(master)
            return "building_snapshot" in monitoring and "last_snapshot_id" in monitoring and \
                   not monitoring["building_snapshot"] and monitoring["last_snapshot_id"] == snapshot_id
        wait(_check_snapshot_built)

        if set_read_only:
            def _check_read_only():
                monitoring = self._get_hydra_monitoring(master)
                return monitoring["read_only"]
            wait(_check_read_only)


@pytest.mark.opensource
class TestHttpProxyBuildSnapshotNoReadonly(TestHttpProxyBuildSnapshotBase):
    @authors("babenko")
    def test_no_read_only(self):
        self._build_snapshot_and_check(False)


@pytest.mark.opensource
class TestHttpProxyBuildSnapshotReadonly(TestHttpProxyBuildSnapshotBase):
    def _check_no_read_only(self):
        monitoring = self._get_hydra_monitoring()
        return "active" in monitoring and monitoring["active"] and not monitoring["read_only"]

    @authors("babenko")
    def test_read_only(self):
        self._build_snapshot_and_check(True)

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        self._master_exit_read_only()

        wait(lambda: self._check_no_read_only())

    @authors("alexkolodezny")
    def test_read_only_proxy_availability(self):
        self._build_snapshot_and_check(True)

        time.sleep(2)

        rsp = requests.get(self._get_proxy_address() + "/ping")
        rsp.raise_for_status()

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        self._master_exit_read_only()

        wait(lambda: self._check_no_read_only())


@pytest.mark.skipif(is_asan_build(), reason="Memory allocation is not reported under ASAN")
@pytest.mark.opensource
class TestHttpProxyHeapUsageStatisticsBase(HttpProxyTestBase):
    NUM_HTTP_PROXIES = 1
    PATH = "//tmp/test"
    USER = "root"
    PARAMS = {"path": PATH, "output_format": "yson", }

    def _execute_command(self, http_method, command_name, params=PARAMS):
        headers = {
            "X-YT-Parameters": yson.dumps(params),
            "X-YT-Header-Format": "<format=text>yson",
            "X-YT-Output-Format": "<format=text>yson",
            "X-YT-User-Name": self.USER,
        }
        rsp = requests.request(
            http_method,
            "{}/api/v4/{}".format(self._get_proxy_address(), command_name),
            headers=headers,
        )

        try_parse_yt_error_headers(rsp)
        return rsp

    def enable_allocation_tags(self, proxy):
        set(f"//sys/{proxy}/@config", {
            "api": {
                "enable_allocation_tags": True,
            },
        })
        wait(lambda: get(f"//sys/{proxy}/@config")["api"]["enable_allocation_tags"])

    def check_memory_usage(self, memory_usage, command):
        tags = ["user", "command"]
        if not memory_usage or len(memory_usage) < len(tags):
            return False

        for tag in tags:
            assert tag in memory_usage

        assert self.USER in memory_usage["user"].keys()
        assert command in memory_usage["command"].keys()

        assert memory_usage["user"][self.USER] > 5 * 1024 ** 2
        assert memory_usage["command"][command] > 5 * 1024 ** 2

        return True


@pytest.mark.skipif(is_asan_build(), reason="Memory allocation is not reported under ASAN")
@pytest.mark.opensource
class TestHttpProxyHeapUsageStatistics(TestHttpProxyHeapUsageStatisticsBase):
    DELTA_PROXY_CONFIG = {
        "heap_profiler": {
            "snapshot_update_period": 50,
        },
        "api": {
            "testing": {
                "heap_profiler": {
                    "allocation_size": 50 * 1024 ** 2,
                    "allocation_release_delay" : 120 * 1000,
                },
            },
        },
    }

    @authors("ni-stoiko")
    @pytest.mark.timeout(120)
    def test_heap_usage_statistics(self):
        http_proxies = ls("//sys/http_proxies")
        assert len(http_proxies) == 1
        http_proxy_agent_orchid = f"//sys/http_proxies/{http_proxies[0]}/orchid/http_proxy"

        self.enable_allocation_tags("http_proxies")
        time.sleep(1)

        create("table", self.PATH)
        write_table(f"<append=%true>{self.PATH}", [{"key": "x"}])

        self._execute_command("GET", "read_table")
        time.sleep(1)
        wait(lambda: self.check_memory_usage(
            get(http_proxy_agent_orchid + "/heap_usage_statistics"),
            "read_table"))

    @authors("ni-stoiko")
    @pytest.mark.timeout(120)
    def test_heap_usage_guages(self):
        self.enable_allocation_tags("http_proxies")
        time.sleep(1)

        http_proxies = ls("//sys/http_proxies")
        assert len(http_proxies) == 1

        create("table", self.PATH)
        write_table(f"<append=%true>{self.PATH}", [{"key": "x"}])

        self._execute_command("GET", "read_table")

        profiler = profiler_factory().at_http_proxy(http_proxies[0])
        command_memory_usage_guage = profiler.gauge("heap_usage/command")
        user_memory_usage_guage = profiler.gauge("heap_usage/user")

        def check(statistics, tag, memory=5 * 1024 ** 2):
            for stat in statistics:
                if stat["tags"] and tag == stat["tags"]:
                    return stat["value"] > memory
            return False

        wait(lambda: check(command_memory_usage_guage.get_all(), {"command": "read_table"}))
        wait(lambda: check(user_memory_usage_guage.get_all(), {"user": self.USER}))


@pytest.mark.skipif(is_asan_build(), reason="Memory allocation is not reported under ASAN")
@pytest.mark.opensource
class TestHttpProxyNullApiTestingOptions(TestHttpProxyHeapUsageStatisticsBase):
    DELTA_PROXY_CONFIG = {
        "heap_profiler": {
            "snapshot_update_period": 50,
        },
    }

    @authors("ni-stoiko")
    def test_null_api_testing_options(self):
        self.enable_allocation_tags("http_proxies")
        create("table", self.PATH)
        write_table(f"<append=%true>{self.PATH}", [{"key": "x"}])
        self._execute_command("GET", "read_table")
