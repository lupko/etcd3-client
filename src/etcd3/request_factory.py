from typing import Any, Iterable, List, Literal, Optional, Union

from typing_extensions import TypeAlias

import etcd3.rpc as etcdrpc
from etcd3.leases import Lease
from etcd3.transactions import Delete, Get, Put, TxCondition, Txn, TxOp
from etcd3.utils import to_bytes

RangeSortOrder: TypeAlias = Literal["asc", "desc", "ascend", "descend"]
RangeSortTarget: TypeAlias = Literal["key", "value", "version", "create", "mod"]


def build_get_range_request(
    key: Union[str, bytes],
    range_end: Optional[Union[str, bytes]] = None,
    sort_order: Optional[RangeSortOrder] = None,
    sort_target: Optional[RangeSortTarget] = None,
    serializable: bool = False,
    keys_only: bool = False,
) -> etcdrpc.RangeRequest:
    range_request = etcdrpc.RangeRequest()
    range_request.key = to_bytes(key)
    range_request.keys_only = keys_only
    if range_end is not None:
        range_request.range_end = to_bytes(range_end)

    if sort_order is None:
        range_request.sort_order = etcdrpc.RangeRequest.NONE
    elif sort_order == "ascend" or sort_order == "asc":
        range_request.sort_order = etcdrpc.RangeRequest.ASCEND
    elif sort_order == "descend" or sort_order == "desc":
        range_request.sort_order = etcdrpc.RangeRequest.DESCEND
    else:
        raise ValueError(f'unknown sort order: "{sort_order}"')

    if sort_target is None or sort_target == "key":
        range_request.sort_target = etcdrpc.RangeRequest.KEY
    elif sort_target == "version":
        range_request.sort_target = etcdrpc.RangeRequest.VERSION
    elif sort_target == "create":
        range_request.sort_target = etcdrpc.RangeRequest.CREATE
    elif sort_target == "mod":
        range_request.sort_target = etcdrpc.RangeRequest.MOD
    elif sort_target == "value":
        range_request.sort_target = etcdrpc.RangeRequest.VALUE
    else:
        raise ValueError(
            'sort_target must be one of "key", "version", "create", "mod" or "value"'
        )

    range_request.serializable = serializable

    return range_request


def build_put_request(
    key: Union[str, bytes],
    value: Union[str, bytes],
    lease: Optional[Union[int, Lease]] = None,
    prev_kv: bool = False,
) -> etcdrpc.PutRequest:
    _lease_id = lease.id if isinstance(lease, Lease) else lease

    put_request = etcdrpc.PutRequest()
    put_request.key = to_bytes(key)
    put_request.value = to_bytes(value)
    put_request.lease = _lease_id or 0
    put_request.prev_kv = prev_kv

    return put_request


def build_delete_request(
    key: Union[str, bytes],
    range_end: Optional[Union[str, bytes]] = None,
    prev_kv: bool = False,
) -> etcdrpc.DeleteRangeRequest:
    delete_request = etcdrpc.DeleteRangeRequest()
    delete_request.key = to_bytes(key)
    delete_request.prev_kv = prev_kv

    if range_end is not None:
        delete_request.range_end = to_bytes(range_end)

    return delete_request


AlarmType: TypeAlias = Literal["none", "no space", "corrupt"]


def build_alarm_request(
    alarm_action: etcdrpc.AlarmRequest.AlarmAction.ValueType,
    member_id: int,
    alarm_type: AlarmType,
) -> etcdrpc.AlarmRequest:
    alarm_request = etcdrpc.AlarmRequest(action=alarm_action, memberID=member_id)

    if alarm_type == "none":
        alarm_request.alarm = etcdrpc.NONE
    elif alarm_type == "no space":
        alarm_request.alarm = etcdrpc.NOSPACE
    elif alarm_type == "corrupt":
        alarm_request.alarm = etcdrpc.CORRUPT
    else:
        raise ValueError("Unknown alarm type: {}".format(alarm_type))

    return alarm_request


def build_tx_request(
    compare: Iterable[TxCondition],
    success: Optional[Iterable[TxOp]],
    failure: Optional[Iterable[TxOp]],
) -> etcdrpc.TxnRequest:
    _compare = [c.build_message() for c in compare]

    success_ops = _ops_to_requests(success)
    failure_ops = _ops_to_requests(failure)

    return etcdrpc.TxnRequest(
        compare=_compare, success=success_ops, failure=failure_ops
    )


def _ops_to_requests(ops: Optional[Iterable[TxOp]]) -> Any:
    """
    Return a list of grpc requests.

    Returns list from an input list of etcd3.transactions.{Put, Get,
    Delete, Txn} objects.
    """
    request_ops: List[etcdrpc.RequestOp] = []

    if ops is None:
        return request_ops

    for op in ops:
        if isinstance(op, Put):
            request_op = etcdrpc.RequestOp(
                request_put=build_put_request(op.key, op.value, op.lease, op.prev_kv)
            )
            request_ops.append(request_op)

        elif isinstance(op, Get):
            request_op = etcdrpc.RequestOp(
                request_range=build_get_range_request(op.key, op.range_end)
            )
            request_ops.append(request_op)

        elif isinstance(op, Delete):
            request_op = etcdrpc.RequestOp(
                request_delete_range=build_delete_request(
                    op.key, op.range_end, op.prev_kv
                )
            )
            request_ops.append(request_op)

        elif isinstance(op, Txn):
            compare = [c.build_message() for c in op.compare]
            success_ops = _ops_to_requests(op.success)
            failure_ops = _ops_to_requests(op.failure)

            request_op = etcdrpc.RequestOp(
                request_txn=etcdrpc.TxnRequest(
                    compare=compare, success=success_ops, failure=failure_ops
                )
            )
            request_ops.append(request_op)

        else:
            raise Exception("Unknown request class {}".format(op.__class__))

    return request_ops


WatchFilterType: TypeAlias = Union[
    etcdrpc.WatchCreateRequest.FilterType.ValueType, Literal["noput", "nodelete"]
]


def _map_watch_filter(
    val: WatchFilterType,
) -> etcdrpc.WatchCreateRequest.FilterType.ValueType:
    if isinstance(val, str):
        if val == "noput":
            return etcdrpc.WatchCreateRequest.NOPUT
        elif val == "nodelete":
            return etcdrpc.WatchCreateRequest.NODELETE
        else:
            raise ValueError(
                "Watch filter can be either 'noput', 'nodelete' literal or WatchCreateRequest FilterType."
            )
    elif isinstance(val, etcdrpc.WatchCreateRequest.FilterType):
        return val
    else:
        raise ValueError(
            "Watch filter can be either 'noput', 'nodelete' literal or WatchCreateRequest FilterType."
        )


def build_create_watch_request(
    key: Union[str, bytes],
    range_end: Optional[Union[str, bytes]] = None,
    start_revision: Optional[int] = None,
    progress_notify: bool = False,
    filters: Optional[Iterable[WatchFilterType]] = None,
    prev_kv: bool = False,
) -> etcdrpc.WatchRequest:
    create_watch = etcdrpc.WatchCreateRequest()
    create_watch.key = to_bytes(key)

    if range_end is not None:
        create_watch.range_end = to_bytes(range_end)

    if start_revision is not None:
        create_watch.start_revision = start_revision

    if progress_notify:
        create_watch.progress_notify = progress_notify

    if filters is not None:
        create_watch.filters.extend([_map_watch_filter(f) for f in filters])

    if prev_kv:
        create_watch.prev_kv = prev_kv

    return etcdrpc.WatchRequest(create_request=create_watch)
