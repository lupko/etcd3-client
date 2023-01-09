import pytest

import etcd3
from etcd3 import etcdrpc


@pytest.fixture(scope="function")
def tx_builder() -> etcd3.TransactionBuilder:
    return etcd3.TransactionBuilder.new()


@pytest.fixture(scope="function")
def other_tx_builder() -> etcd3.TransactionBuilder:
    return etcd3.TransactionBuilder.new()


def test_compare_val(tx_builder):
    compare, _, _ = tx_builder.compare("test").value.eq("val").tx

    assert len(compare) == 1
    assert compare[0].key == b"test"
    assert compare[0].range_end == b""
    assert compare[0].target == etcdrpc.Compare.VALUE
    assert compare[0].result == etcdrpc.Compare.EQUAL
    assert compare[0].value == b"val"


def test_compare_version(tx_builder):
    compare, _, _ = tx_builder.compare("test").version.eq(10).tx

    assert len(compare) == 1
    assert compare[0].key == b"test"
    assert compare[0].target == etcdrpc.Compare.VERSION
    assert compare[0].result == etcdrpc.Compare.EQUAL
    assert compare[0].version == 10


def test_compare_create(tx_builder):
    compare, _, _ = tx_builder.compare("test").create.eq(10).tx

    assert len(compare) == 1
    assert compare[0].key == b"test"
    assert compare[0].target == etcdrpc.Compare.CREATE
    assert compare[0].result == etcdrpc.Compare.EQUAL
    assert compare[0].create_revision == 10


def test_compare_mod(tx_builder):
    compare, _, _ = tx_builder.compare("test").mod.eq(10).tx

    assert len(compare) == 1
    assert compare[0].key == b"test"
    assert compare[0].target == etcdrpc.Compare.MOD
    assert compare[0].result == etcdrpc.Compare.EQUAL
    assert compare[0].mod_revision == 10


def test_compare_lease(tx_builder):
    compare, _, _ = tx_builder.compare("test").lease.eq(10).tx

    assert len(compare) == 1
    assert compare[0].key == b"test"
    assert compare[0].target == etcdrpc.Compare.LEASE
    assert compare[0].result == etcdrpc.Compare.EQUAL
    assert compare[0].lease == 10


def test_compare_ne(tx_builder):
    compare, _, _ = tx_builder.compare("test").version.ne(10).tx

    assert len(compare) == 1
    assert compare[0].key == b"test"
    assert compare[0].target == etcdrpc.Compare.VERSION
    assert compare[0].result == etcdrpc.Compare.NOT_EQUAL
    assert compare[0].version == 10


def test_compare_gt(tx_builder):
    compare, _, _ = tx_builder.compare("test").version.gt(10).tx

    assert len(compare) == 1
    assert compare[0].key == b"test"
    assert compare[0].target == etcdrpc.Compare.VERSION
    assert compare[0].result == etcdrpc.Compare.GREATER
    assert compare[0].version == 10


def test_compare_le(tx_builder):
    compare, _, _ = tx_builder.compare("test").version.le(10).tx

    assert len(compare) == 1
    assert compare[0].key == b"test"
    assert compare[0].target == etcdrpc.Compare.VERSION
    assert compare[0].result == etcdrpc.Compare.LESS
    assert compare[0].version == 10


def test_compare_range_end(tx_builder):
    compare, _, _ = tx_builder.compare("test", "tesu").version.le(10).tx

    assert len(compare) == 1
    assert compare[0].key == b"test"
    assert compare[0].range_end == b"tesu"
    assert compare[0].target == etcdrpc.Compare.VERSION
    assert compare[0].result == etcdrpc.Compare.LESS
    assert compare[0].version == 10


def test_success_put(tx_builder):
    _, success, _ = tx_builder.success.put(key="test", value="val").tx

    assert len(success) == 1
    assert success[0].HasField("request_put")


def test_success_get(tx_builder):
    _, success, _ = tx_builder.success.get(key="test", range_end="tesu").tx

    assert len(success) == 1
    assert success[0].HasField("request_range")


def test_success_delete(tx_builder):
    _, success, _ = tx_builder.success.delete(key="test", range_end="tesu").tx

    assert len(success) == 1
    assert success[0].HasField("request_delete_range")


def test_success_tx(tx_builder, other_tx_builder):
    other_tx_builder.compare(key="test").value.eq("something")
    other_tx_builder.success.put(key="test", value="something_else")
    other_tx_builder.failure.get(key="test")

    compare, success, failure = tx_builder.success.txn(other_tx_builder).tx

    assert not len(compare)
    assert len(success) == 1
    assert not len(failure)

    assert success[0].HasField("request_txn")
    txn = success[0].request_txn

    assert len(txn.compare) == 1
    assert txn.compare[0].key == b"test"
    assert txn.compare[0].target == etcdrpc.Compare.VALUE
    assert txn.compare[0].result == etcdrpc.Compare.EQUAL
    assert txn.compare[0].value == b"something"

    assert len(txn.success) == 1
    assert txn.success[0].HasField("request_put")

    assert len(txn.failure) == 1
    assert txn.failure[0].HasField("request_range")


def test_failure_put(tx_builder):
    _, _, failure = tx_builder.failure.put(key="test", value="val").tx

    assert len(failure) == 1
    assert failure[0].HasField("request_put")


def test_failure_get(tx_builder):
    _, _, failure = tx_builder.failure.get(key="test", range_end="tesu").tx

    assert len(failure) == 1
    assert failure[0].HasField("request_range")


def test_failure_delete(tx_builder):
    _, _, failure = tx_builder.failure.delete(key="test", range_end="tesu").tx

    assert len(failure) == 1
    assert failure[0].HasField("request_delete_range")


def test_multiple_ops(tx_builder):
    tx_builder.success.put(key="test1", value="val1")
    tx_builder.success.put(key="test2", value="val2")
    tx_builder.success.get(key="test3")

    _, success, _ = tx_builder.tx

    assert len(success) == 3
    assert success[0].request_put.key == b"test1"
    assert success[1].request_put.key == b"test2"
    assert success[2].request_range.key == b"test3"
