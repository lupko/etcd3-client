from typing import List, cast

import etcd3.rpc as etcdrpc
from etcd3.base import (
    Alarm,
    GetResult,
    KVMetadata,
    KVResult,
    Member,
    Status,
    TxResponse,
    TxResponses,
    TxResult,
)
from etcd3.client import Etcd3Client, client
from etcd3.events import DeleteEvent, Event, PutEvent
from etcd3.exceptions import (
    ConnectionFailedError,
    ConnectionTimeoutError,
    Etcd3Exception,
    InternalServerError,
    PreconditionFailedError,
    RevisionCompactedError,
    WatchTimedOut,
)
from etcd3.leases import Lease
from etcd3.request_factory import AlarmType, WatchFilterType
from etcd3.transactions import (
    BaseCompare,
    Create,
    Delete,
    Get,
    Mod,
    Put,
    Transactions,
    TxCondition,
    Txn,
    TxOp,
    Value,
    Version,
)
from etcd3.transactions2 import (
    CompareBuilder,
    CompareBuilderStep1,
    OperationBuilder,
    TransactionBuilder,
    TransactionResponse,
)
from etcd3.utils import range_end_for_key
from etcd3.watch import WatchCallback, WatchResponse


def as_get_op_result(response: TxResponse) -> GetResult:
    """
    Given a response to operation done within a transaction, this method will check that it is a response
    to a Get operation and return a properly narrowed type.

    :param response: transaction response
    :return: list of results
    """
    assert isinstance(response, list)

    return response


def as_put_op_result(response: TxResponse) -> etcdrpc.PutResponse:
    """
    Given a response to operation done within a transaction, this method will check that it is a response
    to a Put operation and return PutResponse.

    :param response: transaction response
    :return: list of results
    """
    assert isinstance(response, etcdrpc.ResponseOp) and response.HasField(
        "response_put"
    )

    return response.response_put


def as_del_op_result(response: TxResponse) -> etcdrpc.DeleteRangeResponse:
    """
    Given a response to operation done within a transaction, this method will check that it is a response
    to a Get operation and return DeleteRangeResponse.

    :param response: transaction response
    :return: list of results
    """
    assert isinstance(response, etcdrpc.ResponseOp) and response.HasField(
        "response_delete_range"
    )

    return response.response_delete_range


def as_txn_op_result(response: TxResponse) -> etcdrpc.TxnResponse:
    """
    Given a response to operation done within a transaction, this method will check that it is a response
    to a Get operation and return TxnResponse.

    :param response: transaction response
    :return: list of results
    """
    assert isinstance(response, etcdrpc.ResponseOp) and response.HasField(
        "response_txn"
    )

    return response.response_txn


__version__ = "0.90.0"
