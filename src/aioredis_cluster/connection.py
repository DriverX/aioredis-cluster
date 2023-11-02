import asyncio
import logging
from functools import partial
from types import MappingProxyType
from typing import Iterable, List, Mapping

from aioredis_cluster._aioredis.util import coerced_keys_dict, wait_ok
from aioredis_cluster.abc import AbcChannel, AbcConnection
from aioredis_cluster.aioredis import Channel, ConnectionClosedError
from aioredis_cluster.aioredis import RedisConnection as BaseConnection
from aioredis_cluster.aioredis import RedisError
from aioredis_cluster.aioredis.util import _NOTSET
from aioredis_cluster.command_info.commands import (
    PING_COMMANDS,
    PUBSUB_FAMILY_COMMANDS,
    SHARDED_PUBSUB_COMMANDS,
)
from aioredis_cluster.errors import RedisError
from aioredis_cluster.typedef import PClosableConnection
from aioredis_cluster.util import encode_command

logger = logging.getLogger(__name__)


async def close_connections(conns: Iterable[PClosableConnection]) -> None:
    close_waiters = set()
    for conn in conns:
        conn.close()
        close_waiters.add(asyncio.ensure_future(conn.wait_closed()))
    if close_waiters:
        await asyncio.wait(close_waiters)


class RedisConnection(BaseConnection, AbcConnection):
    _in_pubsub: int

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self._readonly = False
        self._sharded_pubsub_channels = coerced_keys_dict()
        self._loop = asyncio.get_running_loop()
        self._last_use_generation = 0

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
        fut = self.execute("AUTH", username, password)
        return await wait_ok(fut)

    @property
    def sharded_pubsub_channels(self) -> Mapping[str, AbcChannel]:
        return MappingProxyType(self._sharded_pubsub_channels)

    def execute(self, command, *args, encoding=_NOTSET):
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

        is_ping = command in PING_COMMANDS
        if self._in_pubsub and not is_ping:
            raise RedisError("Connection in SUBSCRIBE mode")

        if command in ("SELECT", b"SELECT"):
            cb = partial(self._set_db, args=args)
        elif command in ("MULTI", b"MULTI"):
            cb = self._start_transaction
        elif command in ("EXEC", b"EXEC"):
            cb = partial(self._end_transaction, discard=False)
            encoding = None
        elif command in ("DISCARD", b"DISCARD"):
            cb = partial(self._end_transaction, discard=True)
        else:
            cb = None
        if encoding is _NOTSET:
            encoding = self._encoding

        fut = self._loop.create_future()
        if self._pipeline_buffer is None:
            self._writer.write(encode_command(command, *args))
        else:
            encode_command(command, *args, buf=self._pipeline_buffer)
        self._waiters.append((fut, encoding, cb))
        return fut

    def execute_pubsub(self, command, *channels):
        """Executes redis (p)subscribe/(p)unsubscribe commands.

        Returns asyncio.gather coroutine waiting for all channels/patterns
        to receive answers.
        """
        command = command.upper().strip()
        assert command in PUBSUB_FAMILY_COMMANDS, ("Pub/Sub command expected", command)
        if self._reader is None or self._reader.at_eof():
            raise ConnectionClosedError("Connection closed or corrupted")
        if None in set(channels):
            raise TypeError("args must not contain None")
        if not len(channels):
            raise TypeError("No channels/patterns supplied")

        if command in SHARDED_PUBSUB_COMMANDS:
            is_pattern = False
        else:
            is_pattern = len(command) in (10, 12)

        mkchannel = partial(Channel, is_pattern=is_pattern)
        channels_obj: List[AbcChannel] = [
            ch if isinstance(ch, AbcChannel) else mkchannel(ch) for ch in channels
        ]
        if not all(ch.is_pattern == is_pattern for ch in channels_obj):
            raise ValueError("Not all channels {} match command {}".format(channels, command))

        cmd = encode_command(command, *(ch.name for ch in channels_obj))
        res = []
        for ch in channels_obj:
            fut = self._loop.create_future()
            res.append(fut)
            cb = partial(self._update_pubsub, ch=ch)
            self._waiters.append((fut, None, cb))
        if self._pipeline_buffer is None:
            self._writer.write(cmd)
        else:
            self._pipeline_buffer.extend(cmd)
        return asyncio.gather(*res)

    def get_last_use_generation(self) -> int:
        return self._last_use_generation

    def set_last_use_generation(self, gen: int):
        self._last_use_generation = gen

    def _process_pubsub(self, obj, *, process_waiters: bool = True):
        """Processes pubsub messages."""

        if isinstance(obj, RedisError):
            # case for new pubsub command for example:
            # new ssubscribe to old node of slot
            return self._process_data(obj)

        kind, *args, data = obj
        if kind in (b"subscribe", b"unsubscribe"):
            (chan,) = args
            if process_waiters and self._in_pubsub and self._waiters:
                self._process_data(obj)
            if kind == b"unsubscribe":
                ch = self._pubsub_channels.pop(chan, None)
                if ch:
                    ch.close()
            self._in_pubsub = data
        elif kind in (b"ssubscribe", b"sunsubscribe"):
            (chan,) = args
            if process_waiters and self._in_pubsub and self._waiters:
                self._process_data(obj)
            if kind == b"sunsubscribe":
                ch = self._sharded_pubsub_channels.pop(chan, None)
                if ch:
                    ch.close()
            self._in_pubsub = data
        elif kind in (b"psubscribe", b"punsubscribe"):
            (chan,) = args
            if process_waiters and self._in_pubsub and self._waiters:
                self._process_data(obj)
            if kind == b"punsubscribe":
                ch = self._pubsub_patterns.pop(chan, None)
                if ch:
                    ch.close()
            self._in_pubsub = data
        elif kind == b"message":
            (chan,) = args
            self._pubsub_channels[chan].put_nowait(data)
        elif kind == b"smessage":
            (chan,) = args
            self._sharded_pubsub_channels[chan].put_nowait(data)
        elif kind == b"pmessage":
            pattern, chan = args
            self._pubsub_patterns[pattern].put_nowait((chan, data))
        elif kind == b"pong":
            if process_waiters and self._in_pubsub and self._waiters:
                self._process_data(data or b"PONG")
        else:
            logger.warning("Unknown pubsub message received %r", obj)

    def _update_pubsub(self, obj, *, ch: AbcChannel):
        kind, *pattern, channel, subscriptions = obj
        self._in_pubsub, was_in_pubsub = subscriptions, self._in_pubsub
        if kind == b"subscribe" and channel not in self._pubsub_channels:
            self._pubsub_channels[channel] = ch
        elif kind == b"psubscribe" and channel not in self._pubsub_patterns:
            self._pubsub_patterns[channel] = ch
        elif kind == b"ssubscribe" and channel not in self._sharded_pubsub_channels:
            self._sharded_pubsub_channels[channel] = ch
        if not was_in_pubsub:
            self._process_pubsub(obj, process_waiters=False)
        return obj

    def _do_close(self, exc):
        super()._do_close(exc)

        if self._closed:
            return

        while self._sharded_pubsub_channels:
            _, ch = self._sharded_pubsub_channels.popitem()
            logger.debug("Closing sharded pubsub channel %r", ch)
            ch.close(exc)
