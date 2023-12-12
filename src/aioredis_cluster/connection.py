import asyncio
import logging
import warnings
from collections import deque
from contextlib import contextmanager
from functools import partial
from typing import (
    Any,
    Callable,
    Deque,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Protocol,
    Tuple,
    Union,
)

from aioredis_cluster._aioredis.util import _set_exception, _set_result, decode, wait_ok
from aioredis_cluster.abc import AbcChannel, AbcConnection
from aioredis_cluster.aioredis import (
    Channel,
    ConnectionClosedError,
    ConnectionForcedCloseError,
    MaxClientsError,
    ProtocolError,
    ReadOnlyError,
    ReplyError,
    WatchVariableError,
)
from aioredis_cluster.aioredis.parser import Reader
from aioredis_cluster.aioredis.stream import StreamReader
from aioredis_cluster.aioredis.util import _NOTSET
from aioredis_cluster.command_info.commands import (
    PING_COMMANDS,
    PUBSUB_COMMAND_TO_TYPE,
    PUBSUB_FAMILY_COMMANDS,
    PUBSUB_RESP_KIND_TO_TYPE,
    PUBSUB_SUBSCRIBE_COMMANDS,
    PubSubType,
)
from aioredis_cluster.crc import CrossSlotError, determine_slot
from aioredis_cluster.errors import MovedError, RedisError
from aioredis_cluster.pubsub import PubSubStore
from aioredis_cluster.typedef import PClosableConnection
from aioredis_cluster.util import encode_command, ensure_bytes

logger = logging.getLogger(__name__)


TExecuteCallback = Callable[[Any], Any]
TExecuteErrCallback = Callable[[Exception], Exception]


class ExecuteWaiter(NamedTuple):
    fut: asyncio.Future
    enc: Optional[str]
    cb: Optional[TExecuteCallback]
    err_cb: Optional[TExecuteErrCallback]


class PParserFactory(Protocol):
    def __call__(
        self,
        protocolError: Callable = ProtocolError,
        replyError: Callable = ReplyError,
        encoding: Optional[str] = None,
    ) -> Reader:
        ...


async def close_connections(conns: Iterable[PClosableConnection]) -> None:
    close_waiters = set()
    for conn in conns:
        conn.close()
        close_waiters.add(asyncio.ensure_future(conn.wait_closed()))
    if close_waiters:
        await asyncio.wait(close_waiters)


class RedisConnection(AbcConnection):
    def __init__(
        self,
        reader: StreamReader,
        writer: asyncio.StreamWriter,
        *,
        address: Union[Tuple[str, int], str],
        encoding: Optional[str] = None,
        parser: Optional[PParserFactory] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        if loop is not None:
            warnings.warn("The loop argument is deprecated", DeprecationWarning)
        if parser is None:
            parser = Reader
        assert callable(parser), ("Parser argument is not callable", parser)
        self._reader = reader
        self._writer = writer
        self._address = address
        self._waiters: Deque[ExecuteWaiter] = deque()
        self._reader.set_parser(parser(protocolError=ProtocolError, replyError=ReplyError))
        self._close_msg = ""
        self._db = 0
        self._closing = False
        self._closed = False
        self._close_state = asyncio.Event()
        self._in_transaction: Optional[Deque[Tuple[Optional[str], Optional[Callable]]]] = None
        self._transaction_error: Optional[Exception] = None  # XXX: never used?
        self._pubsub_store = PubSubStore()
        # client side PubSub mode flag
        self._client_in_pubsub = False
        # confirmed PubSub from Redis server via first subscribe reply
        self._server_in_pubsub = False

        self._encoding = encoding
        self._pipeline_buffer: Optional[bytearray] = None
        self._readonly = False
        self._loop = asyncio.get_running_loop()
        self._last_use_generation = 0

        self._reader_task: Optional[asyncio.Task] = self._loop.create_task(self._read_data())
        self._reader_task.add_done_callback(self._on_reader_task_done)

    def __repr__(self):
        return f"<{type(self).__name__} address:{self.address} db:{self.db}>"

    @property
    def readonly(self) -> bool:
        return self._readonly

    async def set_readonly(self, value: bool) -> None:
        """Turn node readonly or readwrite mode."""

        if value:
            fut = self.execute(b"READONLY")
        else:
            fut = self.execute(b"READWRITE")
        await wait_ok(fut)
        self._readonly = value

    async def auth_with_username(self, username: str, password: str) -> bool:
        """Authenticate to server with username and password."""
        fut = self.execute(b"AUTH", username, password)
        return await wait_ok(fut)

    @property
    def pubsub_channels(self) -> Mapping[str, AbcChannel]:
        """Returns read-only channels dict."""
        return self._pubsub_store.channels

    @property
    def pubsub_patterns(self) -> Mapping[str, AbcChannel]:
        """Returns read-only patterns dict."""
        return self._pubsub_store.patterns

    @property
    def sharded_pubsub_channels(self) -> Mapping[str, AbcChannel]:
        """Returns read-only sharded channels dict."""
        return self._pubsub_store.sharded

    async def auth(self, password: str) -> bool:
        """Authenticate to server."""
        fut = self.execute(b"AUTH", password)
        return await wait_ok(fut)

    def execute(self, command, *args, encoding=_NOTSET) -> Any:
        """Executes redis command and returns Future waiting for the answer.

        Raises:
        * TypeError if any of args can not be encoded as bytes.
        * ReplyError on redis '-ERR' responses.
        * ProtocolError when response can not be decoded meaning connection
          is broken.
        * ConnectionClosedError when either client or server has closed the
          connection.
        """
        if self._reader is None or self._reader.at_eof():
            msg = self._close_msg or "Connection closed or corrupted"
            raise ConnectionClosedError(msg)
        if command is None:
            raise TypeError("command must not be None")
        if None in args:
            raise TypeError("args must not contain None")

        command = command.upper().strip()
        if command in PUBSUB_FAMILY_COMMANDS:
            raise ValueError(f"PUB/SUB command {command!r} is prohibited for use with .execute()")

        if encoding is _NOTSET:
            encoding = self._encoding

        is_ping = command in PING_COMMANDS
        if not is_ping and (self._client_in_pubsub or self._server_in_pubsub):
            raise RedisError("Connection in PubSub mode")

        cb: Optional[TExecuteCallback] = None
        if command in ("SELECT", b"SELECT"):
            cb = partial(self._set_db, args=args)
        elif command in ("MULTI", b"MULTI"):
            cb = self._start_transaction
        elif command in ("EXEC", b"EXEC"):
            cb = partial(self._end_transaction, discard=False)
            encoding = None
        elif command in ("DISCARD", b"DISCARD"):
            cb = partial(self._end_transaction, discard=True)

        if self._pipeline_buffer is None:
            self._writer.write(encode_command(command, *args))
        else:
            encode_command(command, *args, buf=self._pipeline_buffer)

        fut = self._loop.create_future()
        self._waiters.append(
            ExecuteWaiter(
                fut=fut,
                enc=encoding,
                cb=cb,
                err_cb=None,
            )
        )

        return fut

    def execute_pubsub(self, command, *channels: Union[bytes, str, AbcChannel]):
        """Executes redis (p|s)subscribe/(p|s)unsubscribe commands.

        Returns asyncio.gather coroutine waiting for all channels/patterns
        to receive answers.
        """
        command = command.upper().strip()
        if command not in PUBSUB_FAMILY_COMMANDS:
            raise ValueError(f"Pub/Sub command expected, not {command!r}")
        if self._reader is None or self._reader.at_eof():
            raise ConnectionClosedError("Connection closed or corrupted")
        if None in set(channels):
            raise TypeError("args must not contain None")

        channel_type = PUBSUB_COMMAND_TO_TYPE[command]
        is_subscribe_command = command in PUBSUB_SUBSCRIBE_COMMANDS
        is_pattern = channel_type is PubSubType.PATTERN
        key_slot = -1
        reply_kind = ensure_bytes(command.lower())

        channels_obj: List[AbcChannel]
        if len(channels) == 0:
            if is_subscribe_command:
                raise ValueError("No channels to (un)subscribe")
            elif channel_type is PubSubType.PATTERN:
                channels_obj = list(self._pubsub_store.patterns.values())
            elif channel_type is PubSubType.SHARDED:
                channels_obj = list(self._pubsub_store.sharded.values())
            else:
                channels_obj = list(self._pubsub_store.channels.values())
        else:
            mkchannel = partial(Channel, is_pattern=is_pattern)
            channels_obj = []
            for channel_name_or_obj in channels:
                if isinstance(channel_name_or_obj, AbcChannel):
                    ch = channel_name_or_obj
                else:
                    ch = mkchannel(channel_name_or_obj)
                # FIXME: processing duplicate channels totally broken in aioredis
                # if ch.name in channels_obj:
                #     raise ValueError(f"Found channel duplicates in {channels!r}")
                if ch.is_pattern != is_pattern:
                    raise ValueError(f"Not all channels {channels!r} match command {command!r}")
                channels_obj.append(ch)

            if channel_type is PubSubType.SHARDED:
                try:
                    key_slot = determine_slot(*(ensure_bytes(ch.name) for ch in channels_obj))
                except CrossSlotError:
                    raise ValueError(
                        f"Not all channels shared one key slot in cluster {channels!r}"
                    ) from None

        cmd = encode_command(command, *(ch.name for ch in channels_obj))
        res: List[Any] = []

        if is_subscribe_command:
            for ch in channels_obj:
                channel_name = ensure_bytes(ch.name)
                self._pubsub_store.channel_subscribe(
                    channel_type=channel_type,
                    channel_name=channel_name,
                    channel=ch,
                    key_slot=key_slot,
                )
                if channel_type is PubSubType.SHARDED:
                    channels_num = self._pubsub_store.sharded_channels_num
                else:
                    channels_num = self._pubsub_store.channels_num
                res.append([reply_kind, channel_name, channels_num])

        # otherwise unsubscribe command
        else:
            for ch in channels_obj:
                channel_name = ensure_bytes(ch.name)
                self._pubsub_store.channel_unsubscribe(
                    channel_type=channel_type,
                    channel_name=channel_name,
                    by_reply=False,
                )
                if channel_type is PubSubType.SHARDED:
                    channels_num = self._pubsub_store.sharded_channels_num
                else:
                    channels_num = self._pubsub_store.channels_num
                res.append([reply_kind, channel_name, channels_num])

        if self._pipeline_buffer is None:
            self._writer.write(cmd)
        else:
            self._pipeline_buffer.extend(cmd)

        if not self._client_in_pubsub and not self._server_in_pubsub:
            if is_subscribe_command:
                # entering to PubSub mode on client side
                self._client_in_pubsub = True

            fut = self._loop.create_future()
            self._waiters.append(
                ExecuteWaiter(
                    fut=fut,
                    enc=None,
                    cb=self._get_execute_pubsub_callback(command, res),
                    err_cb=self._get_execute_pubsub_err_callback(),
                )
            )
        else:
            fut = self._loop.create_future()
            fut.set_result(res)

        return fut

    def get_last_use_generation(self) -> int:
        return self._last_use_generation

    def set_last_use_generation(self, gen: int):
        self._last_use_generation = gen

    def close(self) -> None:
        """Close connection."""
        self._do_close(ConnectionForcedCloseError())

    @property
    def closed(self) -> bool:
        """True if connection is closed."""
        closed = self._closing or self._closed
        if not closed and self._reader and self._reader.at_eof():
            self._closing = closed = True
            self._loop.call_soon(self._do_close, None)
        return closed

    async def wait_closed(self) -> None:
        """Coroutine waiting until connection is closed."""
        await self._close_state.wait()

    @property
    def db(self) -> int:
        """Currently selected db index."""
        return self._db

    @property
    def encoding(self) -> Optional[str]:
        """Current set codec or None."""
        return self._encoding

    @property
    def address(self) -> Union[Tuple[str, int], str]:
        """Redis server address, either host-port tuple or str."""
        return self._address

    @property
    def in_transaction(self) -> bool:
        """Set to True when MULTI command was issued."""
        return self._in_transaction is not None

    @property
    def in_pubsub(self) -> int:
        """Indicates that connection is in PUB/SUB mode.

        This implementation NOT provides the number of subscribed channels
        and provides only boolean flag
        """
        return int(self._client_in_pubsub)

    async def select(self, db: int) -> bool:
        """Change the selected database for the current connection."""
        if not isinstance(db, int):
            raise TypeError("DB must be of int type, not {!r}".format(db))
        if db < 0:
            raise ValueError("DB must be greater or equal 0, got {!r}".format(db))
        fut = self.execute(b"SELECT", db)
        return await wait_ok(fut)

    def _set_db(self, ok, args):
        assert ok in {b"OK", "OK"}, ("Unexpected result of SELECT", ok)
        self._db = args[0]
        return ok

    def _start_transaction(self, ok):
        if self._in_transaction is not None:
            raise RuntimeError("Connection is already in transaction")
        self._in_transaction = deque()
        self._transaction_error = None
        return ok

    def _end_transaction(self, obj: Any, discard: bool) -> Any:
        if self._in_transaction is None:
            raise RuntimeError("Connection is not in transaction")
        self._transaction_error = None
        recall, self._in_transaction = self._in_transaction, None
        recall.popleft()  # ignore first (its _start_transaction)
        if discard:
            return obj

        if not (isinstance(obj, list) or (obj is None and not discard)):
            raise RuntimeError(f"Unexpected MULTI/EXEC result: {obj!r}, {recall!r}")

        # TODO: need to be able to re-try transaction
        if obj is None:
            err = WatchVariableError("WATCH variable has changed")
            obj = [err] * len(recall)

        if len(obj) != len(recall):
            raise RuntimeError(f"Wrong number of result items in mutli-exec: {obj!r}, {recall!r}")

        res = []
        for o, (encoding, cb) in zip(obj, recall):
            if not isinstance(o, RedisError):
                try:
                    if encoding:
                        o = decode(o, encoding)
                    if cb:
                        o = cb(o)
                except Exception as err:
                    res.append(err)
                    continue
            res.append(o)
        return res

    def _do_close(self, exc: Optional[BaseException]) -> None:
        if self._closed:
            return
        self._closed = True
        self._closing = False
        self._writer.transport.close()
        if self._reader_task is not None:
            self._reader_task.cancel()
            self._reader_task = None
        del self._writer
        del self._reader
        self._pipeline_buffer = None

        if exc is not None:
            self._close_msg = str(exc)

        while self._waiters:
            waiter = self._waiters.popleft()
            logger.debug("Cancelling waiter %r", waiter)
            if exc is None:
                _set_exception(waiter.fut, ConnectionForcedCloseError())
            else:
                _set_exception(waiter.fut, exc)

        self._pubsub_store.close(exc)

    def _on_reader_task_done(self, task: asyncio.Task) -> None:
        if not task.cancelled() and task.exception():
            logger.error(
                "Reader task unexpectedly done with expection: %r",
                task.exception(),
                exc_info=task.exception(),
            )
            # prevent RedisConnection stuck in half-closed state
            self._reader_task = None
            self._do_close(ConnectionForcedCloseError())
        self._close_state.set()

    def _is_pubsub_resp(self, obj: Any) -> bool:
        if not isinstance(obj, (tuple, list)):
            return False
        if len(obj) == 0:
            return False
        return obj[0] in PUBSUB_RESP_KIND_TO_TYPE

    async def _read_data(self) -> None:
        """Response reader task."""
        last_error = ConnectionClosedError("Connection has been closed by server")
        while not self._reader.at_eof():
            try:
                obj = await self._reader.readobj()
            except asyncio.CancelledError:
                # NOTE: reader can get cancelled from `close()` method only.
                last_error = RuntimeError("this is unexpected")
                break
            except ProtocolError as exc:
                # ProtocolError is fatal
                # so connection must be closed
                if self._in_transaction is not None:
                    self._transaction_error = exc
                last_error = exc
                break
            except Exception as exc:
                # NOTE: for QUIT command connection error can be received
                #       before response
                last_error = exc
                break
            else:
                if (obj == b"" or obj is None) and self._reader.at_eof():
                    logger.debug("Connection has been closed by server, response: %r", obj)
                    last_error = ConnectionClosedError("Reader at end of file")
                    break

                if isinstance(obj, MaxClientsError):
                    last_error = obj
                    break

                if self._loop.get_debug():
                    logger.debug(
                        "Received reply (client_in_pubsub:%s, server_in_pubsub:%s): %r",
                        self._client_in_pubsub,
                        self._server_in_pubsub,
                        obj,
                    )

                if self._server_in_pubsub:
                    if isinstance(obj, MovedError):
                        if self._pubsub_store.have_slot_channels(obj.info.slot_id):
                            logger.warning(
                                (
                                    "Received MOVED in PubSub mode from %s to %s:%s. "
                                    "Unsubscribe all channels from %d slot",
                                ),
                                self.address,
                                obj.info.host,
                                obj.info.port,
                                obj.info.slot_id,
                            )
                            self._pubsub_store.slot_channels_unsubscribe(obj.info.slot_id)
                    elif isinstance(obj, RedisError):
                        raise obj
                    else:
                        self._process_pubsub(obj)
                else:
                    if isinstance(obj, RedisError):
                        if isinstance(obj, MovedError):
                            if self._pubsub_store.have_slot_channels(obj.info.slot_id):
                                logger.warning(
                                    (
                                        "Received MOVED from %s to %s:%s. "
                                        "Unsubscribe all channels from %d slot",
                                    ),
                                    self.address,
                                    obj.info.host,
                                    obj.info.port,
                                    obj.info.slot_id,
                                )
                                self._pubsub_store.slot_channels_unsubscribe(obj.info.slot_id)
                        elif isinstance(obj, ReplyError):
                            if obj.args[0].startswith("READONLY"):
                                obj = ReadOnlyError(obj.args[0])
                        self._wakeup_waiter_with_exc(obj)
                    else:
                        self._wakeup_waiter_with_result(obj)

        self._closing = True
        self._loop.call_soon(self._do_close, last_error)

    def _wakeup_waiter_with_exc(self, exc: Exception) -> None:
        """Processes command errors."""

        if not self._waiters:
            logger.error("No waiter for process error: %r", exc)
            return

        waiter = self._waiters.popleft()

        if waiter.err_cb is not None:
            try:
                exc = waiter.err_cb(exc)
            except Exception as cb_exc:
                logger.exception("Waiter error callback failed with exception: %r", cb_exc)
                exc = cb_exc

        _set_exception(waiter.fut, exc)
        if self._in_transaction is not None:
            self._transaction_error = exc

    def _wakeup_waiter_with_result(self, result: Any) -> None:
        """Processes command results."""

        if self._loop.get_debug():  # pragma: no cover
            logger.debug("Wakeup first waiter for reply: %r", result)

        if not self._waiters:
            logger.error("No waiter for received reply: %r, %r", type(result), result)
            return

        waiter = self._waiters.popleft()
        self._resolve_waiter_with_result(waiter, result)

    def _resolve_waiter_with_result(self, waiter: ExecuteWaiter, result: Any) -> None:
        if waiter.enc is not None:
            try:
                decoded_result = decode(result, waiter.enc)
            except Exception as exc:
                _set_exception(waiter.fut, exc)
                return
        else:
            decoded_result = result

        del result

        if waiter.cb is not None:
            try:
                converted_result = waiter.cb(decoded_result)
            except Exception as exc:
                _set_exception(waiter.fut, exc)
                return
        else:
            converted_result = decoded_result

        del decoded_result

        _set_result(waiter.fut, converted_result)
        if self._in_transaction is not None:
            self._in_transaction.append((waiter.enc, waiter.cb))

    def _process_pubsub(self, obj: Any) -> Any:
        """Processes pubsub messages.

        This method calls directly on `_read_data` routine
        and used as callback in `execute_pubsub` for first PubSub mode initial reply
        """

        if self._loop.get_debug():  # pragma: no cover
            logger.debug(
                "Process PubSub reply (client_in_pubsub:%s, server_in_pubsub:%s): %r",
                self._client_in_pubsub,
                self._server_in_pubsub,
                obj,
            )

        if isinstance(obj, bytes):
            # process simple bytes as PING reply
            kind = b"pong"
            data = obj
        else:
            kind, *args, data = obj

        channel_name: bytes

        if kind in {b"subscribe", b"psubscribe", b"ssubscribe"}:
            logger.debug("PubSub %s event received: %r", kind, obj)
            (channel_name,) = args
            channel_type = PUBSUB_RESP_KIND_TO_TYPE[kind]
            if self._client_in_pubsub and not self._server_in_pubsub:
                self._server_in_pubsub = True
            # confirm PubSub mode in client side based on server reply and reset pending flag
            self._pubsub_store.confirm_subscribe(channel_type, channel_name)
        elif kind in {b"unsubscribe", b"punsubscribe", b"sunsubscribe"}:
            logger.debug("PubSub %s event received: %r", kind, obj)
            (channel_name,) = args
            channel_type = PUBSUB_RESP_KIND_TO_TYPE[kind]
            self._pubsub_store.channel_unsubscribe(
                channel_type=channel_type,
                channel_name=channel_name,
                by_reply=True,
            )
        elif kind in {b"message", b"smessage", b"pmessage"}:
            if kind == b"pmessage":
                (pattern, channel_name) = args
            else:
                (channel_name,) = args
                pattern = channel_name

            channel_type = PUBSUB_RESP_KIND_TO_TYPE[kind]
            if self._pubsub_store.has_channel(channel_type, pattern):
                channel = self._pubsub_store.get_channel(channel_type, pattern)
                if channel_type is PubSubType.PATTERN:
                    channel.put_nowait((channel_name, data))
                else:
                    channel.put_nowait(data)
            else:
                logger.warning(
                    "No channel %r with type %s for received message", pattern, channel_type
                )
        elif kind == b"pong":
            if not self._waiters:
                logger.error("No PubSub PONG waiters for received data %r", data)
            else:
                # in PubSub mode only PING waiters in this deque
                # see in execute() method `is_ping` condition
                waiter = self._waiters.popleft()
                self._resolve_waiter_with_result(waiter, data or b"PONG")
        else:
            logger.warning("Unknown pubsub message received %r", obj)

        return obj

    @contextmanager
    def _buffered(self):
        # XXX: we must ensure that no await happens
        #   as long as we buffer commands.
        #   Probably we can set some error-raising callback on enter
        #   and remove it on exit
        #   if some await happens in between -> throw an error.
        #   This is creepy solution, 'cause some one might want to await
        #   on some other source except redis.
        #   So we must only raise error we someone tries to await
        #   pending aioredis future
        # One of solutions is to return coroutine instead of a future
        # in `execute` method.
        # In a coroutine we can check if buffering is enabled and raise error.

        # TODO: describe in docs difference in pipeline mode for
        #   conn.execute vs pipeline.execute()
        if self._pipeline_buffer is None:
            self._pipeline_buffer = bytearray()
            try:
                yield self
                buf = self._pipeline_buffer
                self._writer.write(buf)
            finally:
                self._pipeline_buffer = None
        else:
            yield self

    def _get_execute_pubsub_callback(
        self, command: Union[str, bytes], expect_replies: List[Any]
    ) -> TExecuteCallback:
        def callback(server_reply: Any) -> Any:
            # this callback processing only first reply on (p|s)(un)subscribe commands

            server_reply = self._process_pubsub(server_reply)

            if list(server_reply) != expect_replies[0]:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.error(
                        "Unexpected server reply on PubSub on %r: %r, expected %r",
                        command,
                        server_reply,
                        expect_replies[0],
                    )

                exc = RedisError(f"Unexpected server reply on PubSub {command!r}")
                self._loop.call_soon(self._do_close, exc)
                raise exc

            return expect_replies

        return callback

    def _get_execute_pubsub_err_callback(self) -> TExecuteErrCallback:
        def callback(exc: Exception) -> Exception:
            if isinstance(exc, ReplyError):
                # return PubSub mode to closed state if any reply error received
                self._client_in_pubsub = False
            return exc

        return callback
