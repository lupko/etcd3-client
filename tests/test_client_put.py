import pytest

import etcd3


def test_put(client1, keyspace):
    test_key = keyspace("key")
    response = client1.put(test_key, b"value")

    assert response.header.revision > 0
    assert response.prev_kv.key == b""

    _, meta = client1.get_strict(test_key)
    assert meta.create_revision == response.header.revision


def test_put_with_prevkv(client1, keyspace):
    test_key = keyspace("key")
    response = client1.put(test_key, b"value", prev_kv=True)

    assert response.header.revision > 0
    assert response.prev_kv.key == b""


def test_put_repeated(client1, keyspace):
    test_key = keyspace("key")
    initial_response = client1.put(test_key, b"value1")
    second_response = client1.put(test_key, b"value2")

    assert second_response.header.revision > initial_response.header.revision
    assert second_response.prev_kv.key == b""


def test_put_repeated_with_added_lease(client1, keyspace):
    test_key = keyspace("key")
    lease = client1.lease(60)

    client1.put(test_key, b"value1")
    client1.put(test_key, b"value2", lease=lease)

    client1.revoke_lease(lease_id=lease.id)

    with pytest.raises(KeyError):
        client1.get_strict(test_key)


def test_put_repeated_with_prevkv(client1, keyspace):
    test_key = keyspace("key")
    client1.put(test_key, b"value1")
    second_response = client1.put(test_key, b"value2", prev_kv=True)

    assert second_response.prev_kv.key == test_key
    assert second_response.prev_kv.value == b"value1"


def test_put_if_not_exists1(client1, keyspace):
    test_key = keyspace("key")
    added, response = client1.put_if_not_exists(test_key, b"value1")

    assert added
    assert response is not None
    assert isinstance(response, etcd3.etcdrpc.PutResponse)


def test_put_if_not_exists2(client1, keyspace):
    test_key = keyspace("key")
    first, response1 = client1.put_if_not_exists(test_key, b"value1")
    second, response2 = client1.put_if_not_exists(test_key, b"value2")

    assert first
    assert response1 is not None
    assert isinstance(response1, etcd3.etcdrpc.PutResponse)

    assert not second
    assert response2 is None

    value, _ = client1.get_strict(test_key)
    assert value == b"value1"


def test_put_with_lease(client1, keyspace):
    test_key = keyspace("key")

    lease = client1.lease(60)
    added = client1.put(test_key, b"value", lease=lease)

    # key was added
    assert added

    # key still exists
    client1.get_strict(test_key)

    # then lease gets removed
    client1.revoke_lease(lease_id=lease.id)

    # and the key is gone
    with pytest.raises(KeyError):
        client1.get_strict(test_key)


def test_put_if_not_exists_with_lease1(client1, keyspace):
    test_key = keyspace("key")

    lease = client1.lease(60)
    added = client1.put_if_not_exists(test_key, b"value", lease=lease)

    # key was added
    assert added

    # key still exists
    client1.get_strict(test_key)

    # then lease gets removed
    client1.revoke_lease(lease_id=lease.id)

    # and the key is gone
    with pytest.raises(KeyError):
        client1.get_strict(test_key)


def test_put_if_not_exists_with_lease2(client1, keyspace):
    test_key = keyspace("key")
    lease = client1.lease(60)

    # the second put will not happen, thus lease is not in effect
    client1.put_if_not_exists(test_key, b"value1")
    client1.put_if_not_exists(test_key, b"value2", lease=lease)

    value, _ = client1.get_strict(test_key)
    assert value == b"value1"

    client1.revoke_lease(lease_id=lease.id)

    value, _ = client1.get_strict(test_key)
    assert value == b"value1"


def test_replace(client1, keyspace):
    test_key = keyspace("key")

    client1.put(test_key, b"value1")
    replaced, response = client1.replace(test_key, b"value1", b"value2")

    assert replaced
    assert response is not None
    assert isinstance(response, etcd3.etcdrpc.PutResponse)

    value, meta = client1.get_strict(test_key)

    assert value == b"value2"


def test_replace_fail(client1, keyspace):
    test_key = keyspace("key")

    client1.put(test_key, b"value1")
    replaced, response = client1.replace(test_key, b"value2", b"value3")

    assert not replaced
    assert response is None

    value, meta = client1.get_strict(test_key)

    assert value == b"value1"


def test_replace_with_lease(client1, keyspace):
    test_key = keyspace("key")
    lease = client1.lease(60)

    client1.put(test_key, b"value1")
    replaced = client1.replace(test_key, b"value1", b"value2", lease=lease)

    assert replaced

    client1.revoke_lease(lease_id=lease.id)

    with pytest.raises(KeyError):
        client1.get_strict(test_key)
