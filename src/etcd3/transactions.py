from typing import Any, Dict, Iterable, Optional, Union

from typing_extensions import TypeAlias

import etcd3.rpc as etcdrpc
from etcd3.leases import Lease
from etcd3.utils import to_bytes

_OPERATORS: Dict[etcdrpc.Compare.CompareResult.ValueType, str] = {
    etcdrpc.Compare.EQUAL: "==",
    etcdrpc.Compare.NOT_EQUAL: "!=",
    etcdrpc.Compare.LESS: "<",
    etcdrpc.Compare.GREATER: ">",
}


class BaseCompare:
    def __init__(
        self, key: Union[str, bytes], range_end: Optional[Union[str, bytes]] = None
    ):
        """
        Compare a target field of one or more KeyValues against some value.

        For example compare create revision of a KeyValue (specified by key) against some number. Or compare a
        value of some KeyValue (specified by key) against a particular value (bytes).

        :param key: key to compare
        :param range_end: optionally specify end of key range to compare; if you provide this value, then
         all keys in range will be compared using this same condition
        """
        self._key = key
        self._range_end = range_end
        self._value: Optional[Any] = None
        self._op: Optional[etcdrpc.Compare.CompareResult.ValueType] = None

    def __eq__(self, other: Any) -> "BaseCompare":  # type: ignore
        self._check_rhs(other)

        self._value = other
        self._op = etcdrpc.Compare.EQUAL

        return self

    def __ne__(self, other: Any) -> "BaseCompare":  # type: ignore
        self._check_rhs(other)

        self._value = other
        self._op = etcdrpc.Compare.NOT_EQUAL

        return self

    def __lt__(self, other: Any) -> "BaseCompare":
        self._check_rhs(other)

        self._value = other
        self._op = etcdrpc.Compare.LESS

        return self

    def __gt__(self, other: Any) -> "BaseCompare":
        self._check_rhs(other)

        self._value = other
        self._op = etcdrpc.Compare.GREATER

        return self

    @property
    def key(self) -> Union[str, bytes]:
        return self._key

    @property
    def range_end(self) -> Optional[Union[str, bytes]]:
        return self._range_end

    @property
    def value(self) -> Optional[Any]:
        return self._value

    @property
    def op(self) -> Optional[etcdrpc.Compare]:
        return self._value

    def build_message(self) -> etcdrpc.Compare:
        assert self._op is not None
        assert self._value is not None

        compare = etcdrpc.Compare()
        compare.key = to_bytes(self._key)

        if self._range_end is not None:
            compare.range_end = to_bytes(self._range_end)

        compare.result = self._op

        self._build_compare(compare, self._value)
        return compare

    def _build_compare(self, compare: etcdrpc.Compare, value: Any) -> None:
        raise NotImplementedError

    def _check_rhs(self, other: Any) -> None:
        raise NotImplementedError

    def __repr__(self) -> str:
        if self._range_end is None:
            keys = self._key
        else:
            keys = f"[{self._key!r}, {self._range_end!r})"

        operator = _OPERATORS.get(self._op) if self._op is not None else "?"

        return f"{self.__class__}: {keys!r} {operator} '{self._value!r}'"


class Value(BaseCompare):
    def _build_compare(self, compare: etcdrpc.Compare, value: Any) -> None:
        compare.target = etcdrpc.Compare.VALUE
        compare.value = to_bytes(value)

    def _check_rhs(self, other: Any) -> None:
        if not isinstance(other, (str, bytes)):
            raise ValueError(
                "Right hand side for value comparison must be string or bytes."
            )


class Version(BaseCompare):
    def _build_compare(self, compare: etcdrpc.Compare, value: Any) -> None:
        compare.target = etcdrpc.Compare.VERSION
        compare.version = int(value)

    def _check_rhs(self, other: Any) -> None:
        try:
            int(other)
        except ValueError:
            raise ValueError("Right hand side for version comparison must be integer")


class Create(BaseCompare):
    def _build_compare(self, compare: etcdrpc.Compare, value: Any) -> None:
        compare.target = etcdrpc.Compare.CREATE
        compare.create_revision = int(value)

    def _check_rhs(self, other: Any) -> None:
        try:
            int(other)
        except ValueError:
            raise ValueError(
                "Right hand side for create revision comparison must be integer"
            )


class Mod(BaseCompare):
    def _build_compare(self, compare: etcdrpc.Compare, value: Any) -> None:
        compare.target = etcdrpc.Compare.MOD
        compare.mod_revision = int(value)

    def _check_rhs(self, other: Any) -> None:
        try:
            int(other)
        except ValueError:
            raise ValueError(
                "Right hand side for mod revision comparison must be integer"
            )


class Put:
    def __init__(
        self,
        key: Union[str, bytes],
        value: Union[str, bytes],
        lease: Optional[Union[int, Lease]] = None,
        prev_kv: bool = False,
    ):
        self.key = key
        self.value = value
        self.lease = lease
        self.prev_kv = prev_kv


class Get:
    def __init__(
        self, key: Union[str, bytes], range_end: Optional[Union[str, bytes]] = None
    ):
        self.key = key
        self.range_end = range_end


class Delete:
    def __init__(
        self,
        key: Union[str, bytes],
        range_end: Optional[Union[str, bytes]] = None,
        prev_kv: bool = False,
    ):
        self.key = key
        self.range_end = range_end
        self.prev_kv = prev_kv


class Txn:
    def __init__(
        self,
        compare: Iterable["TxCondition"],
        success: Optional[Iterable["TxOp"]] = None,
        failure: Optional[Iterable["TxOp"]] = None,
    ):
        self.compare = compare
        self.success = success
        self.failure = failure


TxCondition: TypeAlias = Union[Value, Create, Mod, Version, BaseCompare]

TxOp: TypeAlias = Union[Get, Put, Delete, Txn]


class Transactions:
    value = Value
    version = Version
    create = Create
    mod = Mod

    put = Put
    get = Get
    delete = Delete
    txn = Txn
