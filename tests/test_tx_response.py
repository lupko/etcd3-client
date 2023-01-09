from typing import Tuple

import pytest
from conftest import KeyFactory

import etcd3
from etcd3 import TransactionBuilder


@pytest.fixture(scope="function")
def tx_response_fixture(
    client1, keyspace
) -> Tuple[etcd3.Etcd3Client, KeyFactory, bytes, int]:
    test_key = keyspace("test")
    response = client1.put(test_key, b"test")
    client1.put(keyspace("test/sub"), b"sub")

    return client1, keyspace, test_key, response.header.revision


def test_add_if_not_exists(tx_response_fixture):
    client1, keyspace, _, _ = tx_response_fixture
    new_key = keyspace("new")

    tx = TransactionBuilder.new()
    tx.compare(new_key).version.eq(0)
    tx.success.put(new_key, value=b"added")

    response = client1.txn(tx)
    assert response.success

    put_response = response.as_put(0)

    assert put_response.header.revision > 0
    assert client1.get_strict(new_key)[0] == b"added"


def test_add_or_get_existing(tx_response_fixture):
    client1, keyspace, existing, existing_rev = tx_response_fixture

    tx = TransactionBuilder.new()
    tx.compare(existing).create.eq(0)
    tx.success.put(existing, value=b"no update")
    tx.failure.get(existing)

    response = client1.txn(tx)
    assert not response.success

    get_response = response.as_get(0)

    assert len(get_response) == 1
    assert get_response[0][0] == b"test"
    assert get_response[0][1].key == existing
    assert get_response[0][1].create_revision == existing_rev


def test_delete_if_value_success(tx_response_fixture):
    client1, keyspace, existing, existing_rev = tx_response_fixture

    tx = TransactionBuilder.new()
    tx.compare(existing).value.eq(b"test")
    tx.success.delete(key=existing)

    response = client1.txn(tx)
    assert response.success

    del_response = response.as_del(0)

    assert del_response.deleted == 1
    assert not len(del_response.prev_kvs)


def test_multiple_ops(tx_response_fixture):
    client1, keyspace, existing, existing_rev = tx_response_fixture
    new_key = keyspace("new")

    tx = TransactionBuilder.new()
    tx.compare(existing).value.eq(b"test")
    tx.success.delete(key=existing)
    tx.success.put(key=new_key, value=b"new")
    tx.success.get(key=keyspace("bogus"))

    response = client1.txn(tx)
    assert response.success

    del_response = response.as_del(0)
    put_response = response.as_put(1)
    get_response = response.as_get(2)

    assert del_response.deleted == 1
    assert put_response.header.revision > existing_rev
    assert not len(get_response)


def test_nested_tx(tx_response_fixture):
    client1, keyspace, existing, existing_rev = tx_response_fixture
    new_key = keyspace("new")

    nested_tx = TransactionBuilder.new()
    nested_tx.success.delete(key=existing)
    nested_tx.success.put(key=new_key, value=b"new")
    nested_tx.success.get(key=keyspace("bogus"))

    response = client1.txn(TransactionBuilder.new().success.txn(nested_tx))
    assert response.success
    nested_response = response.as_txn(0)

    del_response = nested_response.as_del(0)
    put_response = nested_response.as_put(1)
    get_response = nested_response.as_get(2)

    assert del_response.deleted == 1
    assert put_response.header.revision > existing_rev
    assert not len(get_response)


def test_invalid_access(tx_response_fixture):
    client1, keyspace, existing, existing_rev = tx_response_fixture
    new_key = keyspace("new")

    tx = TransactionBuilder.new()
    tx.compare(existing).value.eq(b"test")
    tx.success.delete(key=existing)
    tx.success.put(key=new_key, value=b"new")
    tx.success.get(key=keyspace("bogus"))

    response = client1.txn(tx)

    #

    with pytest.raises(KeyError):
        response.as_get(0)

    with pytest.raises(KeyError):
        response.as_put(0)

    with pytest.raises(KeyError):
        response.as_txn(0)

    #

    with pytest.raises(KeyError):
        response.as_get(1)

    with pytest.raises(KeyError):
        response.as_del(1)

    with pytest.raises(KeyError):
        response.as_txn(1)

    #

    with pytest.raises(KeyError):
        response.as_put(2)

    with pytest.raises(KeyError):
        response.as_del(2)

    with pytest.raises(KeyError):
        response.as_txn(2)
