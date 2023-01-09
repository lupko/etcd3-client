from typing import Any, Callable, Generic, List, Optional, Tuple, TypeVar, Union
from weakref import WeakValueDictionary

import etcd3.rpc as etcdrpc
from etcd3.base import GetResult, KVMetadata
from etcd3.leases import Lease
from etcd3.request_factory import (
    RangeSortOrder,
    RangeSortTarget,
    build_delete_request,
    build_get_range_request,
    build_put_request,
)
from etcd3.utils import to_bytes


class TransactionBuilder:
    def compare(
        self, key: Union[str | bytes], range_end: Optional[Union[str | bytes]] = None
    ) -> "CompareBuilder":
        raise NotImplementedError

    @property
    def success(self) -> "OperationBuilder":
        raise NotImplementedError

    @property
    def failure(self) -> "OperationBuilder":
        raise NotImplementedError

    @property
    def tx(
        self,
    ) -> Tuple[List[etcdrpc.Compare], List[etcdrpc.RequestOp], List[etcdrpc.RequestOp]]:
        raise NotImplementedError

    @property
    def request(self) -> etcdrpc.TxnRequest:
        raise NotImplementedError

    def copy(self) -> "TransactionBuilder":
        raise NotImplementedError

    @staticmethod
    def new() -> "TransactionBuilder":
        return TransactionBuilderImpl(immutable=False)

    @staticmethod
    def immutable() -> "TransactionBuilder":
        return TransactionBuilderImpl(immutable=True)


class OperationBuilder:
    def __init__(self, add: Callable[[etcdrpc.RequestOp], TransactionBuilder]):
        self._add = add

    def put(
        self,
        key: Union[str, bytes],
        value: Union[str, bytes],
        lease: Optional[Union[int, Lease]] = None,
        prev_kv: bool = False,
    ) -> TransactionBuilder:
        req: etcdrpc.PutRequest = build_put_request(
            key=key, value=value, lease=lease, prev_kv=prev_kv
        )

        return self._add(etcdrpc.RequestOp(request_put=req))

    def get(
        self,
        key: Union[str, bytes],
        range_end: Optional[Union[str, bytes]] = None,
        sort_order: Optional[RangeSortOrder] = None,
        sort_target: Optional[RangeSortTarget] = None,
        keys_only: bool = False,
    ) -> TransactionBuilder:
        req: etcdrpc.RangeRequest = build_get_range_request(
            key=key,
            range_end=range_end,
            sort_order=sort_order,
            sort_target=sort_target,
            keys_only=keys_only,
        )

        return self._add(etcdrpc.RequestOp(request_range=req))

    def delete(
        self,
        key: Union[str, bytes],
        range_end: Optional[Union[str, bytes]] = None,
        prev_kv: bool = False,
    ) -> TransactionBuilder:
        req: etcdrpc.DeleteRangeRequest = build_delete_request(
            key=key, range_end=range_end, prev_kv=prev_kv
        )

        return self._add(etcdrpc.RequestOp(request_delete_range=req))

    def txn(self, other: TransactionBuilder) -> TransactionBuilder:
        return self._add(etcdrpc.RequestOp(request_txn=other.request))


TCmp = TypeVar("TCmp")


class CompareBuilderStep1(Generic[TCmp]):
    def __init__(
        self,
        completion: Callable[
            [etcdrpc.Compare.CompareResult.ValueType, TCmp], TransactionBuilder
        ],
        type_check: Callable[[Any], TCmp],
    ):
        self._completion = completion
        self._type_check = type_check

    def eq(self, val: TCmp) -> TransactionBuilder:
        return self._completion(etcdrpc.Compare.EQUAL, self._type_check(val))

    def ne(self, val: TCmp) -> TransactionBuilder:
        return self._completion(etcdrpc.Compare.NOT_EQUAL, self._type_check(val))

    def gt(self, val: TCmp) -> TransactionBuilder:
        return self._completion(etcdrpc.Compare.GREATER, self._type_check(val))

    def le(self, val: TCmp) -> TransactionBuilder:
        return self._completion(etcdrpc.Compare.LESS, self._type_check(val))


class CompareBuilder:
    def __init__(
        self,
        add: Callable[[etcdrpc.Compare], TransactionBuilder],
        key: Union[str | bytes],
        range_end: Optional[Union[str | bytes]],
    ):
        self._add = add
        self._partial: etcdrpc.Compare = etcdrpc.Compare(key=to_bytes(key))

        if range_end is not None:
            self._partial.range_end = to_bytes(range_end)

    @staticmethod
    def _is_int(val: Any) -> int:
        if isinstance(val, int):
            return val

        raise ValueError("Right hand side must be integer.")

    @staticmethod
    def _is_val_type(val: Any) -> Union[str, bytes]:
        if isinstance(val, (str, bytes)):
            return val

        raise ValueError("Right hand side must be string or bytes.")

    @property
    def value(self) -> CompareBuilderStep1[Union[str, bytes]]:
        self._partial.target = etcdrpc.Compare.VALUE

        def _completion(
            result: etcdrpc.Compare.CompareResult.ValueType, val: Union[str, bytes]
        ) -> TransactionBuilder:
            self._partial.result = result
            self._partial.value = to_bytes(val)

            return self._add(self._partial)

        return CompareBuilderStep1(_completion, self._is_val_type)

    @property
    def version(self) -> CompareBuilderStep1[int]:
        self._partial.target = etcdrpc.Compare.VERSION

        def _completion(
            result: etcdrpc.Compare.CompareResult.ValueType, val: int
        ) -> TransactionBuilder:
            self._partial.result = result
            self._partial.version = val

            return self._add(self._partial)

        return CompareBuilderStep1(_completion, self._is_int)

    @property
    def create(self) -> CompareBuilderStep1[int]:
        self._partial.target = etcdrpc.Compare.CREATE

        def _completion(
            result: etcdrpc.Compare.CompareResult.ValueType, val: int
        ) -> TransactionBuilder:
            self._partial.result = result
            self._partial.create_revision = val

            return self._add(self._partial)

        return CompareBuilderStep1(_completion, self._is_int)

    @property
    def mod(self) -> CompareBuilderStep1[int]:
        self._partial.target = etcdrpc.Compare.MOD

        def _completion(
            result: etcdrpc.Compare.CompareResult.ValueType, val: int
        ) -> TransactionBuilder:
            self._partial.result = result
            self._partial.mod_revision = val

            return self._add(self._partial)

        return CompareBuilderStep1(_completion, self._is_int)

    @property
    def lease(self) -> CompareBuilderStep1[int]:
        self._partial.target = etcdrpc.Compare.LEASE

        def _completion(
            result: etcdrpc.Compare.CompareResult.ValueType, val: int
        ) -> TransactionBuilder:
            self._partial.result = result
            self._partial.lease = val

            return self._add(self._partial)

        return CompareBuilderStep1(_completion, self._is_int)


class TransactionBuilderImpl(TransactionBuilder):
    def __init__(
        self,
        immutable: bool = False,
        compare: Optional[List[etcdrpc.Compare]] = None,
        success: Optional[List[etcdrpc.RequestOp]] = None,
        failure: Optional[List[etcdrpc.RequestOp]] = None,
    ) -> None:
        self._immutable = immutable
        self._compare: List[etcdrpc.Compare] = compare or list()
        self._success: List[etcdrpc.RequestOp] = success or list()
        self._failure: List[etcdrpc.RequestOp] = failure or list()

    def add_compare(self, cmp: etcdrpc.Compare) -> "TransactionBuilder":
        assert cmp is not None

        if not self._immutable:
            self._compare.append(cmp)
            return self

        return TransactionBuilderImpl(
            compare=self._compare + [cmp],
            success=self._success.copy(),
            failure=self._failure.copy(),
        )

    def add_success(self, op: etcdrpc.RequestOp) -> "TransactionBuilder":
        assert op is not None

        if not self._immutable:
            self._success.append(op)
            return self

        return TransactionBuilderImpl(
            compare=self._compare.copy(),
            success=self._success + [op],
            failure=self._failure.copy(),
        )

    def add_failure(self, op: etcdrpc.RequestOp) -> "TransactionBuilder":
        assert op is not None

        if not self._immutable:
            self._failure.append(op)
            return self

        return TransactionBuilderImpl(
            compare=self._compare.copy(),
            success=self._success.copy(),
            failure=self._failure + [op],
        )

    def compare(
        self, key: Union[str | bytes], range_end: Optional[Union[str | bytes]] = None
    ) -> "CompareBuilder":
        return CompareBuilder(self.add_compare, key, range_end)

    @property
    def success(self) -> "OperationBuilder":
        return OperationBuilder(self.add_success)

    @property
    def failure(self) -> "OperationBuilder":
        return OperationBuilder(self.add_failure)

    @property
    def tx(
        self,
    ) -> Tuple[List[etcdrpc.Compare], List[etcdrpc.RequestOp], List[etcdrpc.RequestOp]]:
        return self._compare.copy(), self._success.copy(), self._failure.copy()

    @property
    def request(self) -> etcdrpc.TxnRequest:
        return etcdrpc.TxnRequest(
            compare=self._compare.copy(),
            success=self._success.copy(),
            failure=self._failure.copy(),
        )

    def copy(self) -> "TransactionBuilder":
        return TransactionBuilderImpl(
            compare=self._compare.copy(),
            success=self._success.copy(),
            failure=self._failure.copy(),
        )


class TransactionResponse:
    def __init__(
        self, response: etcdrpc.TxnResponse, slice: Optional[Tuple[int, int]] = None
    ):
        self._r = response
        self._gets: WeakValueDictionary[int, GetResult] = WeakValueDictionary()
        self._slice: Tuple[int, int] = slice or (0, len(self._r.responses))

    def _check_idx(self, idx: int) -> None:
        start, end = self._slice

        if idx >= end or idx < start:
            raise KeyError(f"transaction response {idx} out of bounds")

    def _try_get_responses(self, idx: int) -> GetResult:
        res: etcdrpc.ResponseOp = self._r.responses[idx]

        if not res.HasField("response_range"):
            raise KeyError(f"transaction response {idx} is not for a get operation")

        return [
            (kv.value, KVMetadata.create(kv, self._r.header))
            for kv in res.request_range.kvs
        ]

    @property
    def success(self) -> bool:
        return self._r.succeeded

    @property
    def header(self) -> etcdrpc.ResponseHeader:
        return self._r.header

    def as_get(self, idx: int) -> GetResult:
        self._check_idx(idx)

        g = self._gets.get(idx, None)
        if g is None:
            g = self._try_get_responses(idx)
            self._gets[idx] = g

        return g

    def as_put(self, idx: int) -> etcdrpc.PutResponse:
        self._check_idx(idx)
        res: etcdrpc.ResponseOp = self._r.responses[idx]

        if not res.HasField("response_put"):
            raise KeyError(f"transaction response {idx} is not for a put operation")

        return res.response_put

    def as_del(self, idx: int) -> etcdrpc.DeleteRangeResponse:
        self._check_idx(idx)
        res: etcdrpc.ResponseOp = self._r.responses[idx]

        if not res.HasField("response_delete_range"):
            raise KeyError(f"transaction response {idx} is not for a delete operation")

        return res.response_delete_range

    def as_txn(self, idx: int) -> "TransactionResponse":
        self._check_idx(idx)
        res: etcdrpc.ResponseOp = self._r.responses[idx]

        if not res.HasField("response_txn"):
            raise KeyError(
                f"transaction response {idx} is not for a nested transaction operation"
            )

        return TransactionResponse(res.response_txn)

    def __getitem__(self, item):
        print(item)
