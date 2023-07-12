from typing import Any, Callable, Generic, List, Optional, Tuple, TypeVar, Union

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
        """
        Start adding compare condition to the transaction.

        :param key: key, to which the condition applied
        :param range_end: optionally specify range end if you want to compare multiple keys
        :return: builder for comparison
        """
        raise NotImplementedError

    @property
    def success(self) -> "OperationBuilder":
        """
        Start adding operation to perform if compare succeeds.

        :return: operation builder
        """
        raise NotImplementedError

    @property
    def failure(self) -> "OperationBuilder":
        """
        Start adding operation to perform if compare fails.

        :return: operation builder
        """
        raise NotImplementedError

    @property
    def tx(
        self,
    ) -> Tuple[List[etcdrpc.Compare], List[etcdrpc.RequestOp], List[etcdrpc.RequestOp]]:
        """
        :return: transaction parts. tuple of compare, success and failure operations
        """
        raise NotImplementedError

    @property
    def request(self) -> etcdrpc.TxnRequest:
        """
        :return: request that would perform the transaction built so far
        """
        raise NotImplementedError

    def copy(self) -> "TransactionBuilder":
        """
        :return: a copy this builder
        """
        raise NotImplementedError

    @staticmethod
    def new() -> "TransactionBuilder":
        """
        :return: a new, empty, mutable transaction builder. With mutable builder, the same instance of builder is modified as you add the
         compares and various success and failure operations, the same instance will be mutated and built up
        """
        return TransactionBuilderImpl(immutable=False)

    @staticmethod
    def immutable() -> "TransactionBuilder":
        """
        :return: a new, empty, immutable builder. With immutable builder, when you add compare, success or failure operation, a new instance
         of builder will be created and returned. This is useful if you want to build multiple transactions from some common 'foundation'.
        """
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
        """
        Adds a 'put' operation.

        :param key: key to put
        :param value: value to put
        :param lease: optionally lease to associate with the key (provided either as ID of the lease or Lease object)
        :param prev_kv: indicate whether transaction response should include previous kv of the updated key
        :return: the transaction builder to which the operation was added
        """
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
        limit: int = 0,
        revision: int = 0,
    ) -> TransactionBuilder:
        """
        Adds a 'get' operation.

        :param key: key to get
        :param range_end: optionally provide end key (excluded) if you want to get range of keys
        :param sort_order: indicate sort order for the keys
        :param sort_target: indicate the target field used for sorting ("key", "value", "version", "create", "mod")
        :param keys_only: optionally indicate that the response should only contain keys and the values should be omitted
        :param limit: number of keys to get, zero or negative value means no limit. default is zero
        :param revision: optionally specify store revision at which the get should be done; when not specified or lower or equal
         to zero, the get will be done on the latest version of the store
        :return: the transaction builder to which the operation was added
        """
        req: etcdrpc.RangeRequest = build_get_range_request(
            key=key,
            range_end=range_end,
            sort_order=sort_order,
            sort_target=sort_target,
            keys_only=keys_only,
            limit=limit,
            revision=revision,
        )

        return self._add(etcdrpc.RequestOp(request_range=req))

    def delete(
        self,
        key: Union[str, bytes],
        range_end: Optional[Union[str, bytes]] = None,
        prev_kv: bool = False,
    ) -> TransactionBuilder:
        """
        Add a 'delete' operation.

        :param key: key to delete
        :param range_end: optionally provide end key (excluded) up to which you want to delete
        :param prev_kv: whether to return previous values of the deleted keys
        :return: the transaction builder to which the operation was added
        """
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
        """
        Create an 'equal' compare.

        :param val: value to compare to
        :return: the transaction builder to which the compare was added
        """
        return self._completion(etcdrpc.Compare.EQUAL, self._type_check(val))

    def ne(self, val: TCmp) -> TransactionBuilder:
        """
        Create a 'not equal' compare.

        :param val: value to compare to
        :return: the transaction builder to which the compare was added
        """
        return self._completion(etcdrpc.Compare.NOT_EQUAL, self._type_check(val))

    def gt(self, val: TCmp) -> TransactionBuilder:
        """
        Create a 'greater than' compare.

        :param val: value to compare to
        :return: the transaction builder to which the compare was added
        """
        return self._completion(etcdrpc.Compare.GREATER, self._type_check(val))

    def le(self, val: TCmp) -> TransactionBuilder:
        """
        Create a 'less than' compare.

        :param val: value to compare to
        :return: the transaction builder to which the compare was added
        """
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
        """
        Create value comparison.

        :return: comparison condition to build
        """
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
        """
        Create version comparison.

        :return: comparison condition builder
        """
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
        """
        Create a 'create revision' comparison.

        :return: comparison condition builder
        """
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
        """
        Create a 'mod revision' comparison.

        :return: comparison condition builder
        """
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
        """
        Create a 'lease' comparison.

        :return: comparison condition builder
        """
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
    """
    A facade to access transaction response in a convenient way.
    """

    def __init__(
        self, response: etcdrpc.TxnResponse, slice: Optional[Tuple[int, int]] = None
    ):
        self._r = response
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
            for kv in res.response_range.kvs
        ]

    @property
    def success(self) -> bool:
        """
        :return: True if the transaction succeeded (success operations run) or False if failed (failure operations run)
        """
        return self._r.succeeded

    @property
    def header(self) -> etcdrpc.ResponseHeader:
        """
        :return: transaction response header
        """
        return self._r.header

    def as_get(self, idx: int) -> GetResult:
        """
        Returns response for operation at index 'idx'. It is expected that
        the operation at this index was a GET (range) operation

        :param idx: index of operation whose response you want to get
        :return: results of the get operation
        :raises: KeyError: if response is not for a get operation
        """
        self._check_idx(idx)

        return self._try_get_responses(idx)

    def as_put(self, idx: int) -> etcdrpc.PutResponse:
        """
        Returns response for operation at index 'idx'. It is expected that
        the operation at this index was a PUT operation

        :param idx: index of operation whose response you want to get
        :return: results of the put operation
        :raises: KeyError: if response is not for a put operation
        """
        self._check_idx(idx)
        res: etcdrpc.ResponseOp = self._r.responses[idx]

        if not res.HasField("response_put"):
            raise KeyError(f"transaction response {idx} is not for a put operation")

        return res.response_put

    def as_del(self, idx: int) -> etcdrpc.DeleteRangeResponse:
        """
        Returns response for operation at index 'idx'. It is expected that
        the operation at this index was a DELETE (range) operation

        :param idx: index of operation whose response you want to get
        :return: results of the delete operation
        :raises: KeyError: if response is not for a delete operation
        """
        self._check_idx(idx)
        res: etcdrpc.ResponseOp = self._r.responses[idx]

        if not res.HasField("response_delete_range"):
            raise KeyError(f"transaction response {idx} is not for a delete operation")

        return res.response_delete_range

    def as_txn(self, idx: int) -> "TransactionResponse":
        """
        Returns response for operation at index 'idx'. It is expected that
        the operation at this index was a nested transaction

        :param idx: index of operation whose response you want to get
        :return: results of the nested transaction - another TransactionResponse
        :raises: KeyError: if response is not for a nested transaction operation
        """
        self._check_idx(idx)
        res: etcdrpc.ResponseOp = self._r.responses[idx]

        if not res.HasField("response_txn"):
            raise KeyError(
                f"transaction response {idx} is not for a nested transaction operation"
            )

        return TransactionResponse(res.response_txn)
