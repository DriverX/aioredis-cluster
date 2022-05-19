"""
Module for support options and commands from new versions redis
"""

from typing import Awaitable, Callable


class StreamCustomCommandsMixin:
    """Streams commands mixin.

    For commands details see: https://redis.io/commands/?group=stream
    """

    execute: Callable[..., Awaitable]

    def xadd_620(
        self,
        stream: str,
        fields,
        message_id=b"*",
        max_len=None,
        min_id=None,
        exact=False,
        no_mk_stream=False,
        limit=None,
    ):
        """Add a message to a stream.
        With support params from redis 6.2.0
        """
        if max_len is not None and min_id is not None:
            raise ValueError("max_len and min_id options at the same time are not compatible")

        if limit is not None:
            if max_len is None and min_id is None:
                raise ValueError("limit cannot be used without min_id or max_len")
            if exact:
                raise ValueError("limit cannot be used with exact option")

        args = []

        if no_mk_stream:
            args.append(b"NOMKSTREAM")

        if max_len is not None:
            if exact:
                args.extend((b"MAXLEN", max_len))
            else:
                args.extend((b"MAXLEN", b"~", max_len))

        if min_id is not None:
            if exact:
                args.extend((b"MINID", min_id))
            else:
                args.extend((b"MINID", b"~", min_id))

        if limit is not None:
            args.extend((b"LIMIT", limit))

        args.append(message_id)

        for k, v in fields.items():
            args.extend([k, v])

        return self.execute(b"XADD", stream, *args)
