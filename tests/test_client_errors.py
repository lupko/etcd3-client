import grpc
import mock
import pytest

import etcd3


class MockedException(grpc.RpcError):
    def __init__(self, code, details: str = ""):
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self) -> str:
        return self._details


@pytest.fixture(scope="function")
def throwaway_client(etcd_nodes):
    host, port = etcd_nodes[0]

    return etcd3.client(host, port)


def test_internal_exception_on_internal_error(throwaway_client):
    exception = MockedException(grpc.StatusCode.INTERNAL)
    kv_mock = mock.MagicMock()
    kv_mock.Range.side_effect = exception
    throwaway_client.kvstub = kv_mock

    with pytest.raises(etcd3.InternalServerError):
        throwaway_client.get("foo")


def test_connection_failure_exception_on_connection_failure(throwaway_client):
    exception = MockedException(grpc.StatusCode.UNAVAILABLE)
    kv_mock = mock.MagicMock()
    kv_mock.Range.side_effect = exception
    throwaway_client.kvstub = kv_mock

    with pytest.raises(etcd3.ConnectionFailedError):
        throwaway_client.get("foo")


def test_connection_timeout_exception_on_connection_timeout(throwaway_client):
    exception = MockedException(grpc.StatusCode.DEADLINE_EXCEEDED)
    kv_mock = mock.MagicMock()
    kv_mock.Range.side_effect = exception
    throwaway_client.kvstub = kv_mock

    with pytest.raises(etcd3.ConnectionTimeoutError):
        throwaway_client.get("foo")


def test_grpc_exception_on_unknown_code(throwaway_client):
    exception = MockedException(grpc.StatusCode.DATA_LOSS)
    kv_mock = mock.MagicMock()
    kv_mock.Range.side_effect = exception
    throwaway_client.kvstub = kv_mock

    with pytest.raises(grpc.RpcError):
        throwaway_client.get("foo")


def test_funky_timeout(throwaway_client):
    exception = MockedException(grpc.StatusCode.UNKNOWN, "context deadline exceeded")
    kv_mock = mock.MagicMock()
    kv_mock.Range.side_effect = exception
    throwaway_client.kvstub = kv_mock

    with pytest.raises(etcd3.ConnectionTimeoutError):
        throwaway_client.get("foo")


def test_connection_failure_exception_during_paged_get(throwaway_client):
    exception = MockedException(grpc.StatusCode.UNAVAILABLE)
    kv_mock = mock.MagicMock()
    kv_mock.Range.side_effect = exception
    throwaway_client.kvstub = kv_mock

    with pytest.raises(etcd3.ConnectionFailedError):
        _ = list(throwaway_client.get_prefix_paged(key_prefix="test"))


def test_connection_timeout_exception_during_paged_get(throwaway_client):
    exception = MockedException(grpc.StatusCode.DEADLINE_EXCEEDED)
    kv_mock = mock.MagicMock()
    kv_mock.Range.side_effect = exception
    throwaway_client.kvstub = kv_mock

    with pytest.raises(etcd3.ConnectionTimeoutError):
        _ = list(throwaway_client.get_prefix_paged(key_prefix="test"))
