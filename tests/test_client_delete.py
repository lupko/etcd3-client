from typing import Tuple

import pytest
from conftest import KeyFactory

import etcd3


@pytest.fixture(scope="function")
def del_fixture(client1, keyspace) -> Tuple[etcd3.Etcd3Client, KeyFactory]:
    for i in range(10):
        client1.put(keyspace(f"test2/{i}"), f"test1/val/{i}".encode("utf-8"))
        client1.put(keyspace(f"test1/{i}"), f"test1/val/{i}".encode("utf-8"))
        client1.put(keyspace(f"test/{i}"), f"test/val/{i}".encode("utf-8"))

    return client1, keyspace


def test_delete(del_fixture):
    client1, keyspace = del_fixture
    test_key = keyspace("test/0")

    deleted, response = client1.delete(key=test_key)
    assert deleted
    assert response is not None
    assert response.header.revision > 0
    assert response.deleted == 1
    assert not len(response.prev_kvs)


def test_delete_response_with_prev_kv(del_fixture):
    client1, keyspace = del_fixture
    test_key = keyspace("test/0")

    deleted, response = client1.delete(key=test_key, prev_kv=True)
    assert deleted
    assert response is not None
    assert response.header.revision > 0
    assert len(response.prev_kvs) == 1
    assert response.prev_kvs[0].key == test_key


def test_delete_non_existing(del_fixture):
    client1, keyspace = del_fixture
    test_key = keyspace("does/not/exist")

    deleted, response = client1.delete(key=test_key)
    assert not deleted
    assert response is None


def test_delete_prefix(del_fixture):
    client1, keyspace = del_fixture

    test_prefix = keyspace("test1")
    all_prefix = keyspace("")

    response = client1.delete_prefix(prefix=test_prefix)

    assert response.deleted == 10
    assert len(response.prev_kvs) == 0
    assert len(list(client1.get_prefix(test_prefix))) == 0
    assert len(list(client1.get_prefix(all_prefix))) == 20


def test_delete_prefix_with_prevkv(del_fixture):
    client1, keyspace = del_fixture
    test_prefix = keyspace("test1")

    response = client1.delete_prefix(prefix=test_prefix, prev_kv=True)
    assert response.deleted == 10
    assert len(response.prev_kvs) == 10


def test_delete_range(del_fixture):
    client1, keyspace = del_fixture
    first_key = keyspace("test")
    range_end = keyspace("test2")

    response = client1.delete_range(key=first_key, range_end=range_end)

    assert response.deleted == 20
    assert len(response.prev_kvs) == 0
    assert len(list(client1.get_prefix(keyspace("test2")))) == 10
    assert len(list(client1.get_range(first_key, range_end))) == 0


def test_delete_range_with_prevkv(del_fixture):
    client1, keyspace = del_fixture
    first_key = keyspace("test")
    range_end = keyspace("test2")

    response = client1.delete_range(key=first_key, range_end=range_end, prev_kv=True)

    assert response.deleted == 20
    assert len(response.prev_kvs) == 20
