import os
import queue
import time
from typing import List, Union

import pytest

import etcd3

_TIMEOUT = os.environ.get("TEST_ETCD3_WATCH_TIMEOUT", 0.5)


class WatchEventCollector:
    def __init__(self):
        self._q = queue.Queue()

    def cb(self, val: Union[Exception, etcd3.WatchResponse]):
        self._q.put(val)

    def expect(self, response_count: int) -> List[etcd3.WatchResponse]:
        result = []

        while len(result) != response_count:
            result.append(self._q.get(timeout=_TIMEOUT))

        return result

    def expect_none(self) -> None:
        try:
            self._q.get(timeout=_TIMEOUT)

            raise AssertionError
        except queue.Empty:
            return

    def expect_exception(self) -> Exception:
        exc = self._q.get(timeout=_TIMEOUT)

        assert isinstance(exc, Exception)
        return exc


@pytest.fixture(scope="function")
def collector() -> WatchEventCollector:
    return WatchEventCollector()


@pytest.fixture(scope="function")
def other_collector() -> WatchEventCollector:
    return WatchEventCollector()


def test_watch_single_key_put(client1, keyspace, collector):
    test_key = keyspace("test")

    client1.add_watch_callback(key=test_key, callback=collector.cb)

    put_response = client1.put(key=test_key, value=b"test")

    responses = collector.expect(1)
    assert len(responses) == 1
    assert isinstance(responses[0][0], etcd3.PutEvent)

    evt = responses[0][0]

    assert evt.key == test_key
    assert evt.value == b"test"
    assert evt.version == 1
    assert evt.create_revision == put_response.header.revision
    assert evt.mod_revision == put_response.header.revision
    assert evt.lease == 0

    assert evt.event is not None
    assert evt.kv_meta is not None
    assert evt.prev_kv_meta is not None
    assert evt.header is not None

    for event in responses[0]:
        assert event.key == test_key


def test_watch_single_key_repeated_put(client1, keyspace, collector):
    test_key = keyspace("test")

    client1.add_watch_callback(key=test_key, callback=collector.cb, prev_kv=True)

    put_response1 = client1.put(key=test_key, value=b"test1")
    put_response2 = client1.put(key=test_key, value=b"test2")

    responses = collector.expect(2)
    assert len(responses) == 2

    put1 = responses[0][0]
    put2 = responses[1][0]

    assert isinstance(put1, etcd3.PutEvent)
    assert isinstance(put2, etcd3.PutEvent)

    assert put1.key == test_key
    assert put1.value == b"test1"
    assert put1.version == 1
    assert put1.create_revision == put_response1.header.revision
    assert put1.mod_revision == put_response1.header.revision

    assert put2.key == test_key
    assert put2.value == b"test2"
    assert put2.version == 2
    assert put2.create_revision == put_response1.header.revision
    assert put2.mod_revision == put_response2.header.revision

    assert put2.prev_key == put1.key
    assert put2.prev_value == put1.value
    assert put2.prev_version == put1.version
    assert put2.prev_create_revision == put1.create_revision
    assert put2.prev_mod_revision == put1.mod_revision


def test_watch_single_key_noput_filter(client1, keyspace, collector):
    test_key = keyspace("test")

    client1.add_watch_callback(
        key=test_key, callback=collector.cb, filters=("noput",), prev_kv=True
    )

    # put events are filtered out, so nothing comes through about the put
    client1.put(key=test_key, value=b"test")
    collector.expect_none()

    # deletes are not filtered, so they come through
    client1.delete(key=test_key)
    responses = collector.expect(1)

    assert len(responses) == 1
    evt = responses[0][0]

    assert isinstance(evt, etcd3.DeleteEvent)

    assert evt.value == b""
    assert evt.prev_value == b"test"


def test_watch_single_key_nodelete_filter(client1, keyspace, collector):
    test_key = keyspace("test")

    client1.add_watch_callback(
        key=test_key, callback=collector.cb, filters=("nodelete",), prev_kv=True
    )

    client1.put(key=test_key, value=b"test")
    responses = collector.expect(1)

    assert len(responses) == 1
    assert isinstance(responses[0][0], etcd3.PutEvent)

    # deletes are not filtered, so they come through
    client1.delete(key=test_key)
    collector.expect_none()


def test_cancel_watch(client1, keyspace, collector):
    test_key = keyspace("test")

    watch_id = client1.add_watch_callback(
        key=test_key, callback=collector.cb, filters=("nodelete",), prev_kv=True
    )

    client1.put(key=test_key, value=b"test")
    responses = collector.expect(1)
    assert len(responses) == 1
    assert isinstance(responses[0][0], etcd3.PutEvent)

    # cancel watch; no delete event should come in afterwards
    client1.cancel_watch(watch_id)
    client1.delete(key=test_key)
    collector.expect_none()


def test_multiple_watchers(client1, keyspace, collector, other_collector):
    test_key = keyspace("test")

    client1.add_watch_callback(key=test_key, callback=collector.cb)
    client1.add_watch_callback(key=test_key, callback=other_collector.cb)

    client1.put(key=test_key, value=b"test")

    responses = collector.expect(1)
    other_responses = other_collector.expect(1)

    assert len(responses[0]) == len(other_responses[0])


def test_watch_single_key_put_with_lease(client1, keyspace, collector):
    test_key = keyspace("test")

    lease = client1.lease(60)
    client1.add_watch_callback(key=test_key, callback=collector.cb)

    client1.put(key=test_key, value=b"test", lease=lease)

    responses = collector.expect(1)
    assert len(responses) == 1
    assert isinstance(responses[0][0], etcd3.PutEvent)

    evt = responses[0][0]
    assert evt.lease == lease.id


def test_watch_with_compaction(client1, keyspace, collector):
    test_key = keyspace("test")

    put_response1 = client1.put(key=test_key, value=b"test1")
    put_response2 = client1.put(key=test_key, value=b"test1")

    client1.compact(put_response2.header.revision)
    time.sleep(1)

    client1.add_watch_callback(
        key=test_key,
        callback=collector.cb,
        start_revision=put_response1.header.revision,
    )

    exc = collector.expect_exception()
    assert isinstance(exc, etcd3.RevisionCompactedError)

    client1.add_watch_callback(
        key=test_key,
        callback=collector.cb,
        start_revision=put_response2.header.revision,
    )

    response = collector.expect(1)
    assert len(response[0]) == 1


def test_watch_prefix(client1, keyspace, collector):
    client1.add_watch_prefix_callback(
        key_prefix=keyspace("test"), callback=collector.cb
    )

    tx = client1.tx
    client1.transaction(
        compare=[],
        success=[
            tx.put(key=keyspace("test/1"), value=b"test1"),
            tx.put(key=keyspace("test/2"), value=b"test2"),
            tx.put(key=keyspace("test/3"), value=b"test3"),
        ],
    )

    response = collector.expect(1)
    assert len(response[0]) == 3
    assert response[0][0].key == keyspace("test/1")
    assert response[0][1].key == keyspace("test/2")
    assert response[0][2].key == keyspace("test/3")


def test_watch_response(client1, keyspace):
    test_key = keyspace("test")
    put_response = client1.put(test_key, value=b"test")

    # start watching at revision when the first key was added; that way events come in even without some async work
    # being done in the test
    #
    # note: if history compaction runs on the etcd cluster in the meanwhile, then this can result in error
    events, cancel = client1.watch_response(
        test_key, start_revision=put_response.header.revision
    )

    for response in events:
        assert len(response) == 1
        assert isinstance(response[0], etcd3.PutEvent)
        break

    cancel()


def test_watch_response_compacted(client1, keyspace):
    test_key = keyspace("test")
    put_response1 = client1.put(test_key, value=b"test")
    put_response2 = client1.put(test_key, value=b"test")

    client1.compact(put_response2.header.revision)
    events, cancel = client1.watch_response(
        test_key, start_revision=put_response1.header.revision
    )

    with pytest.raises(etcd3.RevisionCompactedError):
        for _ in events:
            pass
