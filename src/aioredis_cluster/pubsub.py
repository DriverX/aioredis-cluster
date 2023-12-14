import logging
from types import MappingProxyType
from typing import Dict, Mapping, Optional, Set, Tuple

from aioredis_cluster._aioredis.util import coerced_keys_dict
from aioredis_cluster.abc import AbcChannel
from aioredis_cluster.command_info.commands import PubSubType
from aioredis_cluster.crc import key_slot as calc_key_slot

logger = logging.getLogger(__name__)


class PubSubStore:
    def __init__(self) -> None:
        self._channels: coerced_keys_dict[AbcChannel] = coerced_keys_dict()
        self._patterns: coerced_keys_dict[AbcChannel] = coerced_keys_dict()
        self._sharded: coerced_keys_dict[AbcChannel] = coerced_keys_dict()
        self._slot_to_sharded: Dict[int, Set[bytes]] = {}
        self._unconfirmed_subscribes: Dict[Tuple[PubSubType, bytes], int] = {}

    @property
    def channels(self) -> Mapping[str, AbcChannel]:
        """Returns read-only channels dict."""
        return MappingProxyType(self._channels)

    @property
    def patterns(self) -> Mapping[str, AbcChannel]:
        """Returns read-only patterns dict."""
        return MappingProxyType(self._patterns)

    @property
    def sharded(self) -> Mapping[str, AbcChannel]:
        """Returns read-only sharded channels dict."""
        return MappingProxyType(self._sharded)

    def channel_subscribe(
        self,
        *,
        channel_type: PubSubType,
        channel_name: bytes,
        channel: AbcChannel,
        key_slot: int,
    ) -> None:
        if channel_type is PubSubType.CHANNEL:
            if channel_name not in self._channels:
                self._channels[channel_name] = channel
        elif channel_type is PubSubType.PATTERN:
            if channel_name not in self._patterns:
                self._patterns[channel_name] = channel
        elif channel_type is PubSubType.SHARDED:
            if key_slot < 0:
                raise ValueError("key_slot cannot be negative for sharded channel")

            if channel_name not in self._sharded:
                self._sharded[channel_name] = channel
                if key_slot not in self._slot_to_sharded:
                    self._slot_to_sharded[key_slot] = {channel_name}
                else:
                    self._slot_to_sharded[key_slot].add(channel_name)
        else:  # pragma: no cover
            raise RuntimeError(f"Unexpected channel_type {channel_type}")

        unconfirmed_key = (channel_type, channel_name)
        if unconfirmed_key not in self._unconfirmed_subscribes:
            self._unconfirmed_subscribes[unconfirmed_key] = 1
        else:
            self._unconfirmed_subscribes[unconfirmed_key] += 1

    def confirm_subscribe(self, channel_type: PubSubType, channel_name: bytes) -> None:
        unconfirmed_key = (channel_type, channel_name)
        val = self._unconfirmed_subscribes.get(unconfirmed_key)
        if val is not None:
            val -= 1
            if val < 0:
                logger.error("Too much PubSub subscribe confirms for %r", unconfirmed_key)
                val = 0

            if val == 0:
                del self._unconfirmed_subscribes[unconfirmed_key]
                if channel_type is PubSubType.SHARDED:
                    # this is counterpart of channel_unsubscribe()
                    # we must remove key_slot -> channel_name entry
                    # if not channels objects exists
                    if channel_name not in self._sharded:
                        self._remove_channel_from_slot_map(channel_name)
            else:
                self._unconfirmed_subscribes[unconfirmed_key] = val
        else:
            logger.error("Unexpected PubSub subscribe confirm for %r", unconfirmed_key)

    def channel_unsubscribe(
        self,
        *,
        channel_type: PubSubType,
        channel_name: bytes,
        by_reply: bool,
    ) -> None:
        have_unconfirmed_subs = (channel_type, channel_name) in self._unconfirmed_subscribes
        # if receive (p|s)unsubscribe reply from Redis
        # and make several sequently subscribe->unsubscribe->subscribe commands
        # - we must ignore all server unsubscribe replies until all subscribes is confirmed
        if by_reply and have_unconfirmed_subs:
            return

        channel: Optional[AbcChannel] = None
        if channel_type is PubSubType.CHANNEL:
            channel = self._channels.pop(channel_name, None)
        elif channel_type is PubSubType.PATTERN:
            channel = self._patterns.pop(channel_name, None)
        elif channel_type is PubSubType.SHARDED:
            channel = self._sharded.pop(channel_name, None)
            # we must remove key_slot -> channel_name entry
            # only if all subscription is confirmed
            if not have_unconfirmed_subs:
                self._remove_channel_from_slot_map(channel_name)

        if channel is not None:
            channel.close()

    def slot_channels_unsubscribe(self, key_slot: int) -> None:
        channel_names = self._slot_to_sharded.pop(key_slot, None)
        if channel_names is None:
            return

        while channel_names:
            channel_name = channel_names.pop()
            self._unconfirmed_subscribes.pop((PubSubType.SHARDED, channel_name), None)
            channel = self._sharded.pop(channel_name, None)
            if channel is not None:
                channel.close()

    def have_slot_channels(self, key_slot: int) -> bool:
        return key_slot in self._slot_to_sharded

    @property
    def channels_total(self) -> int:
        return len(self._channels) + len(self._patterns) + len(self._sharded)

    @property
    def channels_num(self) -> int:
        return len(self._channels) + len(self._patterns)

    @property
    def sharded_channels_num(self) -> int:
        return len(self._sharded)

    def has_channel(self, channel_type: PubSubType, channel_name: bytes) -> bool:
        ret = False
        if channel_type is PubSubType.CHANNEL:
            ret = channel_name in self._channels
        elif channel_type is PubSubType.PATTERN:
            ret = channel_name in self._patterns
        elif channel_type is PubSubType.SHARDED:
            ret = channel_name in self._sharded
        return ret

    def get_channel(self, channel_type: PubSubType, channel_name: bytes) -> AbcChannel:
        if channel_type is PubSubType.CHANNEL:
            channel = self._channels[channel_name]
        elif channel_type is PubSubType.PATTERN:
            channel = self._patterns[channel_name]
        elif channel_type is PubSubType.SHARDED:
            channel = self._sharded[channel_name]
        else:  # pragma: no cover
            raise RuntimeError(f"Unexpected channel type {channel_type!r}")
        return channel

    def close(self, exc: Optional[BaseException]) -> None:
        while self._channels:
            _, ch = self._channels.popitem()
            logger.debug("Closing pubsub channel %r", ch)
            ch.close(exc)
        while self._patterns:
            _, ch = self._patterns.popitem()
            logger.debug("Closing pubsub pattern %r", ch)
            ch.close(exc)
        while self._sharded:
            _, ch = self._sharded.popitem()
            logger.debug("Closing sharded pubsub channel %r", ch)
            ch.close(exc)

        self._slot_to_sharded.clear()
        self._unconfirmed_subscribes.clear()

    def _remove_channel_from_slot_map(self, channel_name: bytes) -> None:
        key_slot = calc_key_slot(channel_name)
        if key_slot in self._slot_to_sharded:
            channels_set = self._slot_to_sharded[key_slot]
            channels_set.discard(channel_name)
            if len(channels_set) == 0:
                del self._slot_to_sharded[key_slot]
