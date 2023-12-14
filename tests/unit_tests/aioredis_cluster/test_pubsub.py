from unittest import mock

import pytest

from aioredis_cluster.abc import AbcChannel
from aioredis_cluster.command_info.commands import PubSubType
from aioredis_cluster.crc import key_slot
from aioredis_cluster.pubsub import PubSubStore


def make_channel_mock():
    return mock.NonCallableMock(AbcChannel)


def test_channel_subscribe__one_channel():
    store = PubSubStore()
    chan = make_channel_mock()
    pchan = make_channel_mock()
    schan = make_channel_mock()

    store.channel_subscribe(
        channel_type=PubSubType.CHANNEL,
        channel_name=b"chan",
        channel=chan,
        key_slot=-1,
    )

    assert store.channels_num == 1
    assert store.channels_total == 1
    assert store.sharded_channels_num == 0
    assert len(store.channels) == 1
    assert len(store.patterns) == 0
    assert len(store.sharded) == 0
    assert store.channels["chan"] is chan

    store.channel_subscribe(
        channel_type=PubSubType.PATTERN,
        channel_name=b"pchan",
        channel=pchan,
        key_slot=-1,
    )

    assert store.channels_num == 2
    assert store.channels_total == 2
    assert store.sharded_channels_num == 0
    assert len(store.channels) == 1
    assert len(store.patterns) == 1
    assert len(store.sharded) == 0
    assert store.patterns["pchan"] is pchan

    schan_key_slot = key_slot(b"schan")
    store.channel_subscribe(
        channel_type=PubSubType.SHARDED,
        channel_name=b"schan",
        channel=schan,
        key_slot=schan_key_slot,
    )

    assert store.channels_num == 2
    assert store.channels_total == 3
    assert store.sharded_channels_num == 1
    assert len(store.channels) == 1
    assert len(store.patterns) == 1
    assert len(store.sharded) == 1
    assert store.sharded["schan"] is schan

    assert schan_key_slot in store._slot_to_sharded
    assert store._slot_to_sharded[schan_key_slot] == {b"schan"}


def test_close__empty():
    store = PubSubStore()
    store.close(None)


def test_close__with_channels():
    store = PubSubStore()

    chan = make_channel_mock()
    pchan = make_channel_mock()
    schan = make_channel_mock()

    store.channel_subscribe(
        channel_type=PubSubType.CHANNEL,
        channel_name=b"chan",
        channel=chan,
        key_slot=-1,
    )
    store.channel_subscribe(
        channel_type=PubSubType.PATTERN,
        channel_name=b"pchan",
        channel=pchan,
        key_slot=-1,
    )
    store.channel_subscribe(
        channel_type=PubSubType.SHARDED,
        channel_name=b"schan",
        channel=schan,
        key_slot=key_slot(b"schan"),
    )

    close_exc = Exception()
    store.close(close_exc)

    assert store.channels_total == 0
    assert len(store._slot_to_sharded) == 0
    assert len(store._unconfirmed_subscribes) == 0

    chan.close.assert_called_once_with(close_exc)
    pchan.close.assert_called_once_with(close_exc)
    schan.close.assert_called_once_with(close_exc)


def test_confirm_subscribe__no_confirms(caplog):
    store = PubSubStore()

    with caplog.at_level("ERROR", "aioredis_cluster.pubsub"):
        store.confirm_subscribe(PubSubType.CHANNEL, b"chan")

    assert len(caplog.records) == 1
    assert "Unexpected PubSub subscribe confirm for" in caplog.records[0].message


def test_confirm_subscribe__simple_confirm(caplog):
    store = PubSubStore()

    chan = make_channel_mock()
    pchan = make_channel_mock()
    schan = make_channel_mock()

    store.channel_subscribe(
        channel_type=PubSubType.CHANNEL,
        channel_name=b"chan",
        channel=chan,
        key_slot=-1,
    )
    store.channel_subscribe(
        channel_type=PubSubType.PATTERN,
        channel_name=b"pchan",
        channel=pchan,
        key_slot=-1,
    )
    schan_key_slot = key_slot(b"schan")
    store.channel_subscribe(
        channel_type=PubSubType.SHARDED,
        channel_name=b"schan",
        channel=schan,
        key_slot=schan_key_slot,
    )

    assert len(store._unconfirmed_subscribes) == 3
    assert schan_key_slot in store._slot_to_sharded

    with caplog.at_level("ERROR", "aioredis_cluster.pubsub"):
        store.confirm_subscribe(PubSubType.SHARDED, b"schan")

    assert len(store._unconfirmed_subscribes) == 2
    assert schan_key_slot in store._slot_to_sharded
    assert len(caplog.records) == 0

    with caplog.at_level("ERROR", "aioredis_cluster.pubsub"):
        store.confirm_subscribe(PubSubType.PATTERN, b"pchan")
        store.confirm_subscribe(PubSubType.CHANNEL, b"chan")

    assert len(store._unconfirmed_subscribes) == 0
    assert len(caplog.records) == 0


@pytest.mark.parametrize("channel_type", list(PubSubType))
def test_confirm_subscribe__resub_confirms(channel_type):
    store = PubSubStore()
    chan = make_channel_mock()
    channel_name = b"chan"
    channel_key_slot = key_slot(channel_name)

    # calls in execute_pubsub() for (P|S)SUBSCRIBE commands
    store.channel_subscribe(
        channel_type=channel_type,
        channel_name=channel_name,
        channel=chan,
        # this is key slot for b"chan"
        key_slot=channel_key_slot,
    )

    # calls in execute_pubsub() for (P|S)UNSUBSCRIBE commands
    store.channel_unsubscribe(
        channel_type=channel_type,
        channel_name=channel_name,
        by_reply=False,
    )

    store.channel_subscribe(
        channel_type=channel_type,
        channel_name=channel_name,
        channel=chan,
        # this is key slot for b"chan"
        key_slot=channel_key_slot,
    )

    assert len(store._unconfirmed_subscribes) == 1
    assert store._unconfirmed_subscribes[(channel_type, channel_name)] == 2
    assert store.channels_total == 1

    # call process_pubsub on receive (p|s)subscribe kind events
    store.confirm_subscribe(channel_type, channel_name)

    assert len(store._unconfirmed_subscribes) == 1
    assert store._unconfirmed_subscribes[(channel_type, channel_name)] == 1
    if channel_type is PubSubType.SHARDED:
        assert channel_key_slot in store._slot_to_sharded

    # call process_pubsub on receive (p|s)unsubscribe kind events
    # this call do nothing because have unconfirmed subscription calls
    store.channel_unsubscribe(
        channel_type=channel_type,
        channel_name=channel_name,
        by_reply=True,
    )

    # second server reply for subscribe command
    store.confirm_subscribe(channel_type, channel_name)
    assert len(store._unconfirmed_subscribes) == 0


@pytest.mark.parametrize("channel_type", list(PubSubType))
def test_confirm_subscribe__resub_and_unsub(channel_type):
    store = PubSubStore()
    chan = make_channel_mock()
    channel_name = b"chan"
    channel_key_slot = key_slot(channel_name)

    for i in range(2):
        store.channel_subscribe(
            channel_type=channel_type,
            channel_name=channel_name,
            channel=chan,
            # this is key slot for b"chan"
            key_slot=channel_key_slot,
        )
        store.channel_unsubscribe(
            channel_type=channel_type,
            channel_name=channel_name,
            by_reply=False,
        )

    assert len(store._unconfirmed_subscribes) == 1
    assert store._unconfirmed_subscribes[(channel_type, channel_name)] == 2
    assert store.channels_total == 0

    # first subscribe reply
    store.confirm_subscribe(channel_type, channel_name)

    assert len(store._unconfirmed_subscribes) == 1
    assert store._unconfirmed_subscribes[(channel_type, channel_name)] == 1
    if channel_type is PubSubType.SHARDED:
        assert channel_key_slot in store._slot_to_sharded

    # first unsubscribe reply
    store.channel_unsubscribe(
        channel_type=channel_type,
        channel_name=channel_name,
        by_reply=True,
    )

    # second subscribe reply
    store.confirm_subscribe(channel_type, channel_name)
    assert len(store._unconfirmed_subscribes) == 0
    if channel_type is PubSubType.SHARDED:
        assert len(store._slot_to_sharded) == 0


@pytest.mark.parametrize("channel_type", list(PubSubType))
def test_confirm_subscribe__unsub_server_push(channel_type):
    store = PubSubStore()
    chan = make_channel_mock()
    channel_name = b"chan"
    channel_key_slot = key_slot(channel_name)

    store.channel_subscribe(
        channel_type=channel_type,
        channel_name=channel_name,
        channel=chan,
        # this is key slot for b"chan"
        key_slot=channel_key_slot,
    )

    # server push (p|s)unsubscribe kind event
    # probably previous sub->unsub attempts
    # we do nothing
    store.channel_unsubscribe(
        channel_type=channel_type,
        channel_name=channel_name,
        by_reply=True,
    )

    assert store.channels_total == 1
    chan.close.assert_not_called()

    # subscribe reply
    store.confirm_subscribe(channel_type, channel_name)

    # server push (p|s)unsubscribe kind event
    # maybe is cluster reshard or node is shutting down
    # we must close all confirmed channels
    store.channel_unsubscribe(
        channel_type=channel_type,
        channel_name=channel_name,
        by_reply=True,
    )

    assert store.channels_total == 0
    chan.close.assert_called_once_with()
    if channel_type is PubSubType.SHARDED:
        assert len(store._slot_to_sharded) == 0


@pytest.mark.parametrize("channel_type", list(PubSubType))
def test_channel_unsubscribe__subscribe_confirm_and_unsubscibe(caplog, channel_type):
    store = PubSubStore()
    chan = make_channel_mock()
    channel_name = b"chan"
    channel_key_slot = key_slot(channel_name)

    store.channel_subscribe(
        channel_type=channel_type,
        channel_name=channel_name,
        channel=chan,
        # this is key slot for b"chan"
        key_slot=channel_key_slot,
    )
    # subscribe reply
    store.confirm_subscribe(channel_type, channel_name)

    store.channel_unsubscribe(
        channel_type=channel_type,
        channel_name=channel_name,
        by_reply=False,
    )

    assert store.channels_total == 0
    assert store.have_slot_channels(channel_key_slot) is False
    assert len(store._slot_to_sharded) == 0
    chan.close.assert_called_once_with()

    with caplog.at_level("WARNING", "aioredis_cluster.pubsub"):
        # server reply for unsubscribe
        store.channel_unsubscribe(
            channel_type=channel_type,
            channel_name=channel_name,
            by_reply=True,
        )

    assert len(caplog.records) == 0


def test_slot_channels_unsubscribe__empty():
    store = PubSubStore()
    store.slot_channels_unsubscribe(1234)


def test_slot_channels_unsubscribe__with_unconfirmed_subscribes():
    # this is unrealistic case but we need check this
    store = PubSubStore()
    chan1 = make_channel_mock()
    channel1_name = b"chan1"
    channel1_key_slot = key_slot(channel1_name)
    chan2 = make_channel_mock()
    channel2_name = b"chan2"
    channel2_key_slot = key_slot(channel2_name)
    chan3 = make_channel_mock()
    channel3_name = b"chan3:{chan1}"
    channel3_key_slot = key_slot(channel3_name)

    assert channel1_key_slot == channel3_key_slot

    store.channel_subscribe(
        channel_type=PubSubType.SHARDED,
        channel_name=channel1_name,
        channel=chan1,
        key_slot=channel1_key_slot,
    )
    store.channel_subscribe(
        channel_type=PubSubType.SHARDED,
        channel_name=channel2_name,
        channel=chan2,
        key_slot=channel2_key_slot,
    )
    store.channel_subscribe(
        channel_type=PubSubType.SHARDED,
        channel_name=channel3_name,
        channel=chan3,
        key_slot=channel3_key_slot,
    )

    assert store.channels_total == 3
    assert store.have_slot_channels(channel1_key_slot) is True
    assert store.have_slot_channels(channel2_key_slot) is True
    assert store.have_slot_channels(0) is False

    store.slot_channels_unsubscribe(channel3_key_slot)

    assert store.channels_total == 1
    assert len(store._unconfirmed_subscribes) == 1
    assert (PubSubType.SHARDED, channel2_name) in store._unconfirmed_subscribes
    chan1.close.assert_called_once_with()
    chan2.close.assert_not_called()
    chan3.close.assert_called_once_with()
