import asyncio
import atexit
import contextlib
import os
import socket
import ssl
import subprocess
import sys
import tempfile
import time
from collections import namedtuple
from urllib.parse import urlencode, urlunparse

import pytest
import pytest_asyncio

from aioredis_cluster import _aioredis as aioredis
from aioredis_cluster._aioredis import sentinel as aioredis_sentinel
from aioredis_cluster.compat.asyncio import timeout as atimeout

TCPAddress = namedtuple("TCPAddress", "host port")

RedisServer = namedtuple("RedisServer", "name tcp_address unixsocket version password")

SentinelServer = namedtuple("SentinelServer", "name tcp_address unixsocket version masters")

# Public fixtures


@pytest.fixture(scope="session")
def event_loop():
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def unused_port():
    """Gets random free port."""

    def fun():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    return fun


@pytest_asyncio.fixture
def create_connection(_closable):
    """Wrapper around aioredis.create_connection."""

    async def f(*args, **kw):
        conn = await aioredis.create_connection(*args, **kw)
        _closable(conn)
        return conn

    return f


@pytest_asyncio.fixture(
    params=[aioredis.create_redis, aioredis.create_redis_pool], ids=["single", "pool"]
)
def create_redis(_closable, request):
    """Wrapper around aioredis.create_redis."""
    factory = request.param

    async def f(*args, **kw):
        redis = await factory(*args, **kw)
        _closable(redis)
        return redis

    return f


@pytest_asyncio.fixture
def create_pool(_closable):
    """Wrapper around aioredis.create_pool."""

    async def f(*args, **kw):
        redis = await aioredis.create_pool(*args, **kw)
        _closable(redis)
        return redis

    return f


@pytest_asyncio.fixture
def create_sentinel(_closable):
    """Helper instantiating RedisSentinel client."""

    async def f(*args, **kw):
        # make it fail fast on slow CIs (if timeout argument is ommitted)
        kw.setdefault("timeout", 0.001)
        client = await aioredis_sentinel.create_sentinel(*args, **kw)
        _closable(client)
        return client

    return f


@pytest_asyncio.fixture
async def pool(create_pool, server):
    """Returns RedisPool instance."""
    pool = await create_pool(server.tcp_address)
    return pool


@pytest_asyncio.fixture
async def redis(create_redis, server, add_async_finalizer):
    """Returns Redis client instance."""

    redis = await create_redis(server.tcp_address)
    add_async_finalizer(redis.flushall())
    return redis


@pytest_asyncio.fixture
async def redis_sentinel(create_sentinel, sentinel):
    """Returns Redis Sentinel client instance."""

    redis_sentinel = await create_sentinel([sentinel.tcp_address], timeout=2)
    ping_result = await redis_sentinel.ping()
    assert ping_result == b"PONG"
    return redis_sentinel


@pytest_asyncio.fixture
async def add_async_finalizer():
    finalizers = []

    def adder(finalizer):
        finalizers.append(finalizer)

    try:
        yield adder
    finally:
        for finalizer in finalizers:
            await finalizer


@pytest_asyncio.fixture
async def _closable():
    conns = []

    async def close():
        waiters = []
        while conns:
            conn = conns.pop(0)
            conn.close()
            waiters.append(conn.wait_closed())
        if waiters:
            await asyncio.gather(*waiters)

    try:
        yield conns.append
    finally:
        await close()


@pytest_asyncio.fixture(scope="session")
def server(start_server):
    """Starts redis-server instance."""
    return start_server("A")


@pytest_asyncio.fixture(scope="session")
def serverB(start_server):
    """Starts redis-server instance."""
    return start_server("B")


@pytest_asyncio.fixture(scope="session")
def sentinel(start_sentinel, request, start_server):
    """Starts redis-sentinel instance with one master -- masterA."""
    # Adding master+slave for normal (no failover) tests:
    master_no_fail = start_server("master-no-fail")
    start_server("slave-no-fail", slaveof=master_no_fail)
    # Adding master+slave for failover test;
    masterA = start_server("masterA")
    start_server("slaveA", slaveof=masterA)
    return start_sentinel("main", masterA, master_no_fail)


@pytest_asyncio.fixture(params=["path", "query"])
def server_tcp_url(server, request):
    def make(**kwargs):
        netloc = "{0.host}:{0.port}".format(server.tcp_address)
        path = ""
        if request.param == "path":
            if "password" in kwargs:
                netloc = ":{0}@{1.host}:{1.port}".format(kwargs.pop("password"), server.tcp_address)
            if "db" in kwargs:
                path = "/{}".format(kwargs.pop("db"))
        query = urlencode(kwargs)
        return urlunparse(("redis", netloc, path, "", query, ""))

    return make


@pytest.fixture
def server_unix_url(server):
    def make(**kwargs):
        query = urlencode(kwargs)
        return urlunparse(("unix", "", server.unixsocket, "", query, ""))

    return make


# Internal stuff #


def pytest_addoption(parser):
    parser.addoption(
        "--redis-server",
        default=[],
        action="append",
        help="Path to redis-server executable," " defaults to `%(default)s`",
    )
    parser.addoption(
        "--ssl-cafile", default="tests/ssl/cafile.crt", help="Path to testing SSL CA file"
    )
    parser.addoption(
        "--ssl-dhparam", default="tests/ssl/dhparam.pem", help="Path to testing SSL DH params file"
    )
    parser.addoption(
        "--ssl-cert", default="tests/ssl/cert.pem", help="Path to testing SSL CERT file"
    )
    parser.addoption("--uvloop", default=False, action="store_true", help="Run tests with uvloop")


def _read_server_version(redis_bin):
    args = [redis_bin, "--version"]
    with subprocess.Popen(args, stdout=subprocess.PIPE) as proc:
        version = proc.stdout.readline().decode("utf-8")
    for part in version.split():
        if part.startswith("v="):
            break
    else:
        raise RuntimeError("No version info can be found in {}".format(version))
    return tuple(map(int, part[2:].split(".")))


@contextlib.contextmanager
def config_writer(path):
    with open(path, "wt") as f:

        def write(*args):
            print(*args, file=f)

        yield write


REDIS_SERVERS = []
VERSIONS = {}


def format_version(srv):
    return "redis_v{}".format(".".join(map(str, VERSIONS[srv])))


_ready_check_log_entries = [
    "The server is now ready to accept connections ",
    "Ready to accept connections tcp",
]
_slave_ready_check_log_entries = [
    "sync: Finished with success",
    "MASTER <-> REPLICA sync: Finished with success",
]


@pytest.fixture(scope="session")
def start_server(_proc, request, unused_port, server_bin):
    """Starts Redis server instance.

    Caches instances by name.
    ``name`` param -- instance alias
    ``config_lines`` -- optional list of config directives to put in config
        (if no config_lines passed -- no config will be generated,
         for backward compatibility).
    """

    version = _read_server_version(server_bin)
    verbose = request.config.getoption("-v") > 3

    servers = {}

    def timeout(t):
        end = time.time() + t
        while time.time() <= end:
            yield True
        raise RuntimeError("Redis startup timeout expired")

    def maker(name, config_lines=None, *, slaveof=None, password=None):
        # assert slaveof is None or isinstance(slaveof, RedisServer), slaveof
        if name in servers:
            return servers[name]

        port = unused_port()
        tcp_address = TCPAddress("localhost", port)
        if sys.platform == "win32":
            unixsocket = None
        else:
            unixsocket = "/tmp/aioredis.{}.sock".format(port)
        dumpfile = "dump-{}.rdb".format(port)
        data_dir = tempfile.gettempdir()
        dumpfile_path = os.path.join(data_dir, dumpfile)
        stdout_file = os.path.join(data_dir, "aioredis.{}.stdout".format(port))
        tmp_files = [dumpfile_path, stdout_file]
        if config_lines:
            config = os.path.join(data_dir, "aioredis.{}.conf".format(port))
            with config_writer(config) as write:
                write("daemonize no")
                write('save ""')
                write("dir ", data_dir)
                write("dbfilename", dumpfile)
                write("port", port)
                if version >= (7, 2):
                    write("locale-collate", '"C"')
                if unixsocket:
                    write("unixsocket", unixsocket)
                    tmp_files.append(unixsocket)
                if password:
                    write('requirepass "{}"'.format(password))
                write("# extra config")
                if version >= (7, 0):
                    write("enable-debug-command yes")
                for line in config_lines:
                    write(line)
                if slaveof is not None:
                    if version >= (7, 0):
                        write("replicaof {0.tcp_address.host} {0.tcp_address.port}".format(slaveof))
                    else:
                        write("slaveof {0.tcp_address.host} {0.tcp_address.port}".format(slaveof))
                    if password:
                        write('masterauth "{}"'.format(password))
            args = [server_bin, config]
            tmp_files.append(config)
        else:
            args = [server_bin]
            args += [
                "--daemonize",
                "no",
                "--save",
                '""',
                "--dir",
                data_dir,
                "--dbfilename",
                dumpfile,
                "--port",
                str(port),
            ]

            if version >= (7, 2):
                args += [
                    "--locale-collate",
                    "C",
                ]

            if password:
                args += [
                    "--requirepass",
                    f"{password}",
                ]
            if slaveof is not None:
                if version >= (7, 0):
                    args += ["--replicaof"]
                else:
                    args += ["--slaveof"]
                args += [
                    str(slaveof.tcp_address.host),
                    str(slaveof.tcp_address.port),
                ]
                if password:
                    args += [
                        "--masterauth",
                        f"{password}",
                    ]

            if version >= (7, 0):
                args += ["--enable-debug-command", "yes"]

            if unixsocket:
                args += [
                    "--unixsocket",
                    unixsocket,
                ]

        if verbose:
            print(f"stdout file: {stdout_file}")
            print("Redis args: " + " ".join(args))

        f = open(stdout_file, "w")
        # f = open("redis.log", "w")
        atexit.register(f.close)
        with open(stdout_file, "r") as f_ro:
            proc = _proc(
                *args,
                shell=False,
                stdout=f,
                stderr=subprocess.STDOUT,
                _clear_tmp_files=tmp_files,
            )

            redis_is_ready = False
            slave_is_ready = False
            for _ in timeout(10):
                if proc.poll() is not None and proc.returncode != 0:
                    f_ro.seek(0)
                    print("args:", server_bin, " ".join(args), file=sys.stderr)
                    print("out:", f_ro.read(), file=sys.stderr)
                    raise RuntimeError("Process terminated")
                log = f_ro.readline()
                if log and verbose:
                    print(name, ":", log, end="")
                for ready_check_entry in _ready_check_log_entries:
                    if ready_check_entry in log:
                        redis_is_ready = True
                        break
                if redis_is_ready:
                    break

            if slaveof is not None:
                for _ in timeout(10):
                    log = f_ro.readline()
                    if log and verbose:
                        print(name, ":", log, end="")
                    for ready_check_entry in _slave_ready_check_log_entries:
                        if ready_check_entry in log:
                            slave_is_ready = True
                            break
                    if slave_is_ready:
                        break
        info = RedisServer(name, tcp_address, unixsocket, version, password)
        servers.setdefault(name, info)
        return info

    return maker


@pytest.fixture(scope="session")
def start_sentinel(_proc, request, unused_port, server_bin):
    """Starts Redis Sentinel instances."""
    version = _read_server_version(server_bin)
    verbose = request.config.getoption("-v") > 3

    sentinels = {}

    def timeout(t):
        end = time.time() + t
        while time.time() <= end:
            yield True
        raise RuntimeError("Redis startup timeout expired")

    def maker(
        name,
        *masters,
        quorum=1,
        noslaves=False,
        down_after_milliseconds=3000,
        failover_timeout=1000,
    ):
        key = (name,) + masters
        if key in sentinels:
            return sentinels[key]
        port = unused_port()
        tcp_address = TCPAddress("localhost", port)
        data_dir = tempfile.gettempdir()
        config = os.path.join(data_dir, "aioredis-sentinel.{}.conf".format(port))
        stdout_file = os.path.join(data_dir, "aioredis-sentinel.{}.stdout".format(port))
        tmp_files = [config, stdout_file]
        if sys.platform != "win32":
            unixsocket = os.path.join(data_dir, "aioredis-sentinel.{}.sock".format(port))
            tmp_files.append(unixsocket)

        with config_writer(config) as write:
            write("daemonize no")
            write('save ""')
            write("port", port)
            if unixsocket:
                write("unixsocket", unixsocket)
            write("loglevel debug")
            if version >= (7, 0):
                write("enable-debug-command yes")
            for master in masters:
                write("sentinel monitor", master.name, "127.0.0.1", master.tcp_address.port, quorum)
                write("sentinel down-after-milliseconds", master.name, down_after_milliseconds)
                write("sentinel failover-timeout", master.name, failover_timeout)
                write("sentinel auth-pass", master.name, master.password)

        args = [
            server_bin,
            config,
            "--sentinel",
        ]

        if version >= (7, 2):
            args += [
                "--locale-collate",
                "C",
            ]

        if verbose:
            print(f"stdout file: {stdout_file}")
            print("Redis args: " + " ".join(args))

        # sys.exit(1)

        f = open(stdout_file, "w")
        atexit.register(f.close)
        proc = _proc(
            *args,
            stdout=f,
            stderr=subprocess.STDOUT,
            _clear_tmp_files=tmp_files,
        )
        # XXX: wait sentinel see all masters and slaves;
        all_masters = {m.name for m in masters}
        if noslaves:
            all_slaves = set()
        else:
            all_slaves = {m.name for m in masters}
        with open(stdout_file, "r") as f:
            for _ in timeout(30):
                assert proc.poll() is None, ("Process terminated", proc.returncode)
                log = f.readline()

                if not log:
                    time.sleep(0.5)
                    continue

                if verbose:
                    print(name, ":", log, end="")

                for m in masters:
                    if "# +monitor master {}".format(m.name) in log:
                        all_masters.discard(m.name)
                    if "* +slave slave" in log and "@ {}".format(m.name) in log:
                        all_slaves.discard(m.name)
                if not all_masters and not all_slaves:
                    break
            else:
                raise RuntimeError("Could not start Sentinel")

        masters = {m.name: m for m in masters}
        info = SentinelServer(name, tcp_address, unixsocket, version, masters)
        sentinels.setdefault(key, info)
        return info

    return maker


@pytest.fixture(scope="session")
def ssl_proxy(_proc, request, unused_port):
    by_port = {}

    cafile = os.path.abspath(request.config.getoption("--ssl-cafile"))
    certfile = os.path.abspath(request.config.getoption("--ssl-cert"))
    dhfile = os.path.abspath(request.config.getoption("--ssl-dhparam"))
    assert os.path.exists(cafile), "Missing SSL CA file, run `make certificate` to generate new one"
    assert os.path.exists(
        certfile
    ), "Missing SSL CERT file, run `make certificate` to generate new one"
    assert os.path.exists(
        dhfile
    ), "Missing SSL DH params, run `make certificate` to generate new one"

    ssl_ctx = ssl.create_default_context(cafile=cafile)
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    ssl_ctx.load_dh_params(dhfile)

    def sockat(unsecure_port):
        if unsecure_port in by_port:
            return by_port[unsecure_port]

        secure_port = unused_port()
        _proc(
            "/usr/bin/socat",
            "openssl-listen:{port},"
            "dhparam={param},"
            "cert={cert},verify=0,fork".format(port=secure_port, param=dhfile, cert=certfile),
            "tcp-connect:localhost:{}".format(unsecure_port),
        )
        time.sleep(1)  # XXX
        by_port[unsecure_port] = secure_port, ssl_ctx
        return secure_port, ssl_ctx

    return sockat


@pytest.fixture(scope="session")
def _proc():
    processes = []
    tmp_files = set()

    def run(*commandline, _clear_tmp_files=(), **kwargs):
        proc = subprocess.Popen(commandline, **kwargs)
        processes.append(proc)
        tmp_files.update(_clear_tmp_files)
        return proc

    try:
        yield run
    finally:
        while processes:
            proc = processes.pop(0)
            proc.terminate()
            proc.wait()
        for path in tmp_files:
            try:
                os.remove(path)
            except OSError:
                pass


async def _wait_coro(corofunc, kwargs, timeout):
    async with atimeout(timeout):
        return await corofunc(**kwargs)


def pytest_collection_modifyitems(session, config, items):
    skip_by_version = []
    for item in items:
        marker = item.get_closest_marker("redis_version")
        if marker is not None:
            try:
                version = VERSIONS[item.callspec.getparam("server_bin")]
            except (KeyError, ValueError, AttributeError):
                # TODO: throw noisy warning
                continue
            if version < marker.kwargs["version"]:
                skip_by_version.append(item)
                item.add_marker(pytest.mark.skip(reason=marker.kwargs["reason"]))
        if "ssl_proxy" in item.fixturenames:
            item.add_marker(
                pytest.mark.skipif(
                    "not os.path.exists('/usr/bin/socat')",
                    reason="socat package required (apt-get install socat)",
                )
            )

    if len(items) != len(skip_by_version):
        for i in skip_by_version:
            items.remove(i)

    for item in items:
        if not item.get_closest_marker("asyncio") and asyncio.iscoroutinefunction(item.function):
            item.add_marker(pytest.mark.asyncio)


def pytest_configure(config):
    bins = config.getoption("--redis-server")[:]
    cmd = "which redis-server"
    if not bins:
        with os.popen(cmd) as pipe:
            path = pipe.read().rstrip()
        assert path, "There is no redis-server on your computer." " Please install it first"
        REDIS_SERVERS[:] = [path]
    else:
        REDIS_SERVERS[:] = bins

    VERSIONS.update({srv: _read_server_version(srv) for srv in REDIS_SERVERS})
    assert VERSIONS, ("Expected to detect redis versions", REDIS_SERVERS)

    class DynamicFixturePlugin:
        @pytest.fixture(scope="session", params=REDIS_SERVERS, ids=format_version)
        def server_bin(self, request):
            """Common for start_server and start_sentinel
            server bin path parameter.
            """
            return request.param

    config.pluginmanager.register(DynamicFixturePlugin(), "server-bin-fixture")

    if config.getoption("--uvloop"):
        try:
            import uvloop
        except ImportError:
            raise RuntimeError("Can not import uvloop, make sure it is installed")
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
