from typing import Tuple

import pytest
from conftest import KeyFactory

import etcd3
import etcd3.rpc as etcdrpc


@pytest.fixture(scope="function")
def txn_fixture(client1, keyspace) -> Tuple[etcd3.Etcd3Client, KeyFactory, bytes, int]:
    test_key = keyspace("test")
    response = client1.put(test_key, b"test")
    client1.put(keyspace("test/sub"), b"sub")

    return client1, keyspace, test_key, response.header.revision


def test_add_if_not_exists(txn_fixture):
    client1, keyspace, _, _ = txn_fixture
    new_key = keyspace("new")
    tx = client1.tx

    success, response = client1.transaction(
        compare=[tx.create(new_key) == 0], success=[tx.put(new_key, value=b"value")]
    )

    assert success
    assert len(response) == 1

    assert isinstance(response[0], etcdrpc.ResponseOp)
    assert response[0].response_put.header.revision > 0

    assert client1.get_strict(new_key)[0] == b"value"


def test_add_or_get_existing(txn_fixture):
    client1, keyspace, existing, existing_rev = txn_fixture
    tx = client1.tx

    success, response = client1.transaction(
        compare=[tx.create(existing) == 0],
        success=[tx.put(existing, value=b"value")],
        failure=[tx.get(existing)],
    )

    assert not success
    assert len(response) == 1

    assert isinstance(response[0], list)
    assert len(response[0]) == 1
    value, meta = response[0][0]

    assert value == b"test"
    assert meta.key == existing
    assert meta.create_revision == existing_rev


def test_delete_if_value_success(txn_fixture):
    client1, keyspace, existing, existing_rev = txn_fixture
    tx = client1.tx

    success, response = client1.transaction(
        compare=[tx.value(existing) == b"test"],
        success=[tx.delete(key=existing)],
        failure=[],
    )

    assert success
    assert len(response) == 1

    assert isinstance(response[0], etcdrpc.ResponseOp)
    assert response[0].response_delete_range.deleted == 1


def test_delete_if_value_success_with_prevkv(txn_fixture):
    client1, keyspace, existing, existing_rev = txn_fixture
    tx = client1.tx

    success, response = client1.transaction(
        compare=[tx.value(existing) == b"test"],
        success=[tx.delete(key=existing, prev_kv=True)],
        failure=[],
    )

    assert success
    assert len(response) == 1

    assert isinstance(response[0], etcdrpc.ResponseOp)
    assert response[0].response_delete_range.deleted == 1
    assert len(response[0].response_delete_range.prev_kvs) == 1


def test_delete_if_value_fail(txn_fixture):
    client1, keyspace, existing, existing_rev = txn_fixture
    tx = client1.tx

    success, response = client1.transaction(
        compare=[tx.value(existing) == b"something"],
        success=[tx.delete(key=existing)],
        failure=[tx.get(existing)],
    )

    assert not success
    assert len(response) == 1

    assert isinstance(response[0], list)
    value, meta = response[0][0]

    assert value == b"test"
    assert meta.key == existing


def test_get_if_revision(txn_fixture):
    client1, keyspace, existing, existing_rev = txn_fixture
    tx = client1.tx

    success, response = client1.transaction(
        compare=[tx.mod(existing) == existing_rev],
        success=[tx.get(key=existing)],
        failure=[],
    )

    assert success
    assert len(response) == 1
    assert isinstance(response[0], list)
    value, meta = response[0][0]

    assert value == b"test"
    assert meta.mod_revision == existing_rev


def test_tx_get_range(txn_fixture):
    client1, keyspace, existing, existing_rev = txn_fixture
    tx = client1.tx

    success, response = client1.transaction(
        compare=[],
        success=[tx.get(key=existing, range_end=etcd3.range_end_for_key(existing))],
        failure=[],
    )

    assert success
    assert len(response) == 1
    assert isinstance(response[0], list)
    assert len(response[0]) == 2


def test_tx_delete_range(txn_fixture):
    client1, keyspace, existing, existing_rev = txn_fixture
    tx = client1.tx

    success, response = client1.transaction(
        compare=[],
        success=[tx.delete(key=existing, range_end=etcd3.range_end_for_key(existing))],
        failure=[],
    )

    assert success
    assert len(response) == 1
    assert isinstance(response[0], etcdrpc.ResponseOp)
    assert response[0].response_delete_range.deleted == 2
    assert len(response[0].response_delete_range.prev_kvs) == 0


def test_tx_delete_range_with_prevkv(txn_fixture):
    client1, keyspace, existing, existing_rev = txn_fixture
    tx = client1.tx

    success, response = client1.transaction(
        compare=[],
        success=[
            tx.delete(
                key=existing, range_end=etcd3.range_end_for_key(existing), prev_kv=True
            )
        ],
        failure=[],
    )

    assert success
    assert len(response) == 1
    assert isinstance(response[0], etcdrpc.ResponseOp)
    assert response[0].response_delete_range.deleted == 2
    assert len(response[0].response_delete_range.prev_kvs) == 2


def test_get_if_not_revision(txn_fixture):
    client1, keyspace, existing, existing_rev = txn_fixture
    tx = client1.tx

    success, response = client1.transaction(
        compare=[tx.create(existing) != existing_rev], failure=[tx.get(key=existing)]
    )

    assert not success
    assert len(response) == 1
    assert isinstance(response[0], list)
    value, meta = response[0][0]

    assert value == b"test"
    assert meta.mod_revision == existing_rev


def test_nested_tx_success(txn_fixture):
    client1, keyspace, existing, existing_rev = txn_fixture
    tx = client1.tx

    success, responses = client1.transaction(
        compare=[],
        success=[
            tx.txn(
                compare=[tx.create(existing) == existing_rev],
                success=[tx.put(existing, value=b"updated")],
            )
        ],
    )

    assert success
    assert len(responses) == 1
    assert isinstance(responses[0], etcdrpc.ResponseOp)
    assert len(responses[0].response_txn.responses) == 1
    assert responses[0].response_txn.responses[0].response_put.header.revision > 0

    value, _ = client1.get_strict(existing)
    assert value == b"updated"


def test_nested_tx_fail(txn_fixture):
    client1, keyspace, existing, existing_rev = txn_fixture
    tx = client1.tx

    success, responses = client1.transaction(
        compare=[],
        success=[
            tx.txn(
                compare=[tx.create(existing) != existing_rev],
                success=[tx.put(existing, value=b"updated")],
            )
        ],
    )

    assert success
    assert len(responses) == 1
    assert isinstance(responses[0], etcdrpc.ResponseOp)

    assert not responses[0].response_txn.succeeded


def test_cond_with_range(txn_fixture):
    client1, keyspace, existing, existing_rev = txn_fixture
    tx = client1.tx

    # this test does not make that much sense, but demonstrates that the compare can be coded over
    # a range of keys

    success, _ = client1.transaction(
        compare=[
            tx.create(key=existing, range_end=etcd3.range_end_for_key(existing)) != 0
        ],
        success=[tx.delete(key=existing, range_end=etcd3.range_end_for_key(existing))],
    )

    assert success
    assert len(list(client1.get_prefix(existing))) == 0


def test_create_cond():
    with pytest.raises(ValueError):
        _ = etcd3.Create(key="test") == "bad_rhs"

    msg = (etcd3.Create(key="test") == 1).build_message()
    assert msg.key == b"test"
    assert msg.create_revision == 1
    assert msg.result == etcdrpc.Compare.EQUAL
    assert msg.target == etcdrpc.Compare.CREATE

    msg = (etcd3.Create(key="test") != 2).build_message()
    assert msg.key == b"test"
    assert msg.create_revision == 2
    assert msg.result == etcdrpc.Compare.NOT_EQUAL
    assert msg.target == etcdrpc.Compare.CREATE

    msg = (etcd3.Create(key="test") > 3).build_message()
    assert msg.key == b"test"
    assert msg.create_revision == 3
    assert msg.result == etcdrpc.Compare.GREATER
    assert msg.target == etcdrpc.Compare.CREATE

    msg = (etcd3.Create(key="test") < 4).build_message()
    assert msg.key == b"test"
    assert msg.create_revision == 4
    assert msg.result == etcdrpc.Compare.LESS
    assert msg.target == etcdrpc.Compare.CREATE


def test_mod_cond():
    with pytest.raises(ValueError):
        _ = etcd3.Mod(key="test") == "bad_rhs"

    msg = (etcd3.Mod(key="test") == 1).build_message()
    assert msg.key == b"test"
    assert msg.mod_revision == 1
    assert msg.result == etcdrpc.Compare.EQUAL
    assert msg.target == etcdrpc.Compare.MOD

    msg = (etcd3.Mod(key="test") != 2).build_message()
    assert msg.key == b"test"
    assert msg.mod_revision == 2
    assert msg.result == etcdrpc.Compare.NOT_EQUAL
    assert msg.target == etcdrpc.Compare.MOD

    msg = (etcd3.Mod(key="test") > 3).build_message()
    assert msg.key == b"test"
    assert msg.mod_revision == 3
    assert msg.result == etcdrpc.Compare.GREATER
    assert msg.target == etcdrpc.Compare.MOD

    msg = (etcd3.Mod(key="test") < 4).build_message()
    assert msg.key == b"test"
    assert msg.mod_revision == 4
    assert msg.result == etcdrpc.Compare.LESS
    assert msg.target == etcdrpc.Compare.MOD


def test_version_cond():
    with pytest.raises(ValueError):
        _ = etcd3.Version(key="test") == "bad_rhs"

    msg = (etcd3.Version(key="test") == 1).build_message()
    assert msg.key == b"test"
    assert msg.version == 1
    assert msg.result == etcdrpc.Compare.EQUAL
    assert msg.target == etcdrpc.Compare.VERSION

    msg = (etcd3.Version(key="test") != 2).build_message()
    assert msg.key == b"test"
    assert msg.version == 2
    assert msg.result == etcdrpc.Compare.NOT_EQUAL
    assert msg.target == etcdrpc.Compare.VERSION

    msg = (etcd3.Version(key="test") > 3).build_message()
    assert msg.key == b"test"
    assert msg.version == 3
    assert msg.result == etcdrpc.Compare.GREATER
    assert msg.target == etcdrpc.Compare.VERSION

    msg = (etcd3.Version(key="test") < 4).build_message()
    assert msg.key == b"test"
    assert msg.version == 4
    assert msg.result == etcdrpc.Compare.LESS
    assert msg.target == etcdrpc.Compare.VERSION


def test_version_value_cond():
    with pytest.raises(ValueError):
        _ = etcd3.Value(key="test") == 0

    msg = (etcd3.Value(key="test") == b"").build_message()
    assert msg.key == b"test"
    assert msg.value == b""
    assert msg.result == etcdrpc.Compare.EQUAL
    assert msg.target == etcdrpc.Compare.VALUE

    msg = (etcd3.Value(key="test") != b"val").build_message()
    assert msg.key == b"test"
    assert msg.value == b"val"
    assert msg.result == etcdrpc.Compare.NOT_EQUAL
    assert msg.target == etcdrpc.Compare.VALUE

    msg = (etcd3.Value(key="test") > "abc").build_message()
    assert msg.key == b"test"
    assert msg.value == b"abc"
    assert msg.result == etcdrpc.Compare.GREATER
    assert msg.target == etcdrpc.Compare.VALUE

    msg = (etcd3.Value(key="test") < "0").build_message()
    assert msg.key == b"test"
    assert msg.value == b"0"
    assert msg.result == etcdrpc.Compare.LESS
    assert msg.target == etcdrpc.Compare.VALUE


def test_cond_repr():
    assert str(etcd3.Value(key="test") == b"")
    assert str(etcd3.Value(key="test", range_end="tesu") == b"")
    assert str(etcd3.Create(key="test") == 0)
    assert str(etcd3.Mod(key="test") == 0)
    assert str(etcd3.Version(key="test") == 0)
