import functools
import inspect
import queue
import threading
from typing import (
    Any,
    BinaryIO,
    Callable,
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
)

import grpc
import grpc._channel

import etcd3.rpc as etcdrpc
from etcd3.base import Alarm, KVMetadata, KVResult, Member, Status, TxResponse, TxResult
from etcd3.events import Event
from etcd3.exceptions import (
    ConnectionFailedError,
    ConnectionTimeoutError,
    InternalServerError,
    PreconditionFailedError,
    WatchTimedOut,
)
from etcd3.leases import Lease
from etcd3.request_factory import (
    AlarmType,
    RangeSortOrder,
    RangeSortTarget,
    WatchFilterType,
    build_alarm_request,
    build_delete_request,
    build_get_range_request,
    build_put_request,
    build_tx_request,
)
from etcd3.transactions import Transactions, TxCondition, TxOp
from etcd3.transactions2 import TransactionBuilder, TransactionResponse
from etcd3.utils import get_secure_creds, range_end_for_key
from etcd3.watch import WatchCallback, Watcher, WatchResponse

_EXCEPTIONS_BY_CODE = {
    grpc.StatusCode.INTERNAL: InternalServerError,
    grpc.StatusCode.UNAVAILABLE: ConnectionFailedError,
    grpc.StatusCode.DEADLINE_EXCEEDED: ConnectionTimeoutError,
    grpc.StatusCode.FAILED_PRECONDITION: PreconditionFailedError,
}


def _translate_exception(exc: grpc.RpcError) -> None:
    call = cast(grpc.Call, exc)
    code = call.code()
    details = call.details()

    # for some reason, sometimes, on timeout grpc raises error with status UNKNOWN and detail
    # "context deadline exceeded"; it can still raise DEADLINE_EXCEEDED but
    if code == grpc.StatusCode.UNKNOWN and details == "context deadline exceeded":
        raise ConnectionTimeoutError from exc

    exception = _EXCEPTIONS_BY_CODE.get(code)

    if exception is None:
        raise

    raise exception from exc


FuncT = TypeVar("FuncT", bound=Callable[..., Any])


def _handle_errors(f: FuncT) -> FuncT:
    if inspect.isgeneratorfunction(f):

        @functools.wraps(f)
        def handler(*args: Any, **kwargs: Any) -> Any:
            try:
                for data in f(*args, **kwargs):
                    yield data
            except grpc.RpcError as exc:
                _translate_exception(exc)

    else:

        @functools.wraps(f)
        def handler(*args: Any, **kwargs: Any) -> Any:
            try:
                return f(*args, **kwargs)
            except grpc.RpcError as exc:
                _translate_exception(exc)

    return cast(FuncT, handler)


class _EtcdTokenCallCredentials(grpc.AuthMetadataPlugin):
    """Metadata wrapper for raw access token credentials."""

    def __init__(self, access_token: str):
        self._access_token = access_token

    def __call__(
        self,
        context: grpc.AuthMetadataContext,
        callback: grpc.AuthMetadataPluginCallback,
    ) -> None:
        metadata = (("token", self._access_token),)
        callback(metadata, None)


class Etcd3Client:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 2379,
        ca_cert: Optional[str] = None,
        cert_key: Optional[str] = None,
        cert_cert: Optional[str] = None,
        timeout: Optional[int | float] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        grpc_options: Optional[List[Tuple[str, Any]]] = None,
    ):

        self._url = "{host}:{port}".format(host=host, port=port)
        self.metadata: Optional[Tuple[Tuple[str, str], ...]] = None

        cert_params = [c is not None for c in (cert_cert, cert_key)]
        if ca_cert is not None:
            if all(cert_params):
                credentials = get_secure_creds(ca_cert, cert_key, cert_cert)
                self.uses_secure_channel = True
                self.channel = grpc.secure_channel(
                    self._url, credentials, options=grpc_options
                )
            elif any(cert_params):
                # some of the cert parameters are set
                raise ValueError(
                    "to use a secure channel ca_cert is required by itself, "
                    "or cert_cert and cert_key must both be specified."
                )
            else:
                credentials = get_secure_creds(ca_cert, None, None)
                self.uses_secure_channel = True
                self.channel = grpc.secure_channel(
                    self._url, credentials, options=grpc_options
                )
        else:
            self.uses_secure_channel = False
            self.channel = grpc.insecure_channel(self._url, options=grpc_options)

        self.timeout = timeout
        self.call_credentials: Optional[grpc.CallCredentials] = None

        cred_params = [c is not None for c in (user, password)]

        if user is not None and password is not None:
            self.auth_stub = etcdrpc.AuthStub(self.channel)
            auth_request = etcdrpc.AuthenticateRequest(name=user, password=password)

            resp: etcdrpc.AuthenticateResponse = self.auth_stub.Authenticate(
                auth_request, self.timeout
            )
            self.metadata = (("token", resp.token),)
            self.call_credentials = grpc.metadata_call_credentials(
                _EtcdTokenCallCredentials(resp.token)
            )

        elif any(cred_params):
            raise Exception(
                "if using authentication credentials both user and password "
                "must be specified."
            )

        self.kvstub = etcdrpc.KVStub(self.channel)
        self.watcher = Watcher(
            etcdrpc.WatchStub(self.channel),
            timeout=self.timeout,
            call_credentials=self.call_credentials,
            metadata=self.metadata,
        )
        self.clusterstub = etcdrpc.ClusterStub(self.channel)
        self.leasestub = etcdrpc.LeaseStub(self.channel)
        self.maintenancestub = etcdrpc.MaintenanceStub(self.channel)

        self.transactions = Transactions()
        self.tx = self.transactions

    def close(self) -> None:
        """Call the GRPC channel close semantics."""
        if hasattr(self, "channel"):
            self.channel.close()

    def __enter__(self) -> "Etcd3Client":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    @_handle_errors
    def get_response(
        self,
        key: Union[str, bytes],
        serializable: bool = False,
        revision: int = 0,
        timeout_override: Optional[int] = None,
    ) -> etcdrpc.RangeResponse:
        """Get the value of a key from etcd."""
        range_request = build_get_range_request(
            key, serializable=serializable, revision=revision
        )

        return self.kvstub.Range(
            range_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

    def get(
        self,
        key: Union[str, bytes],
        serializable: bool = False,
        revision: int = 0,
        timeout_override: Optional[int] = None,
    ) -> Union[Tuple[None, None], KVResult]:
        """
        Get the value and metadata of a key from etcd. If the key does not exist, then both value and metadata
        will be None.

        example usage:

        .. code-block:: python

            >>> import etcd3
            >>> etcd = etcd3.client()
            >>> etcd.get('/thing/key')
            'hello world'

        :param key: key in etcd to get
        :param serializable: whether to allow serializable reads. This can
            result in stale reads
        :param revision: optionally specify store revision at which the get should be done; when not specified or lower or equal
         to zero, the get will be done on the latest version of the store
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        :returns: value of key and metadata
        :rtype: bytes, ``KVMetadata``
        """
        range_response = self.get_response(
            key, serializable, revision=revision, timeout_override=timeout_override
        )

        if range_response.count < 1:
            return None, None

        kv: etcdrpc.KeyValue = range_response.kvs.pop()
        return kv.value, KVMetadata.create(kv, range_response.header)

    def get_strict(
        self,
        key: Union[str, bytes],
        serializable: bool = False,
        revision: int = 0,
        timeout_override: Optional[int] = None,
    ) -> KVResult:
        """
        Get the value of a key from etcd; fails with KeyError if no such key exists.

        example usage:

        .. code-block:: python

            >>> import etcd3
            >>> etcd = etcd3.client()
            >>> etcd.get_strict('/thing/key')
            'hello world'

        :param key: key in etcd to get
        :param serializable: whether to allow serializable reads. This can
            result in stale reads
        :param revision: optionally specify store revision at which the get should be done; when not specified or lower or equal
         to zero, the get will be done on the latest version of the store
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        :returns: value of key and metadata
        :rtype: bytes, ``KVMetadata``
        :raise: KeyError if the requested key is not
        """
        range_response = self.get_response(
            key, serializable, revision=revision, timeout_override=timeout_override
        )

        if range_response.count < 1:
            raise KeyError(key)

        kv: etcdrpc.KeyValue = range_response.kvs.pop()
        return kv.value, KVMetadata.create(kv, range_response.header)

    @_handle_errors
    def get_prefix_response(
        self,
        key_prefix: Union[str, bytes],
        keys_only: bool = False,
        sort_order: Optional[RangeSortOrder] = None,
        sort_target: Optional[RangeSortTarget] = None,
        serializable: bool = False,
        revision: int = 0,
        timeout_override: Optional[int] = None,
    ) -> etcdrpc.RangeResponse:
        """Get a range of keys with a prefix."""
        range_request = build_get_range_request(
            key=key_prefix,
            range_end=range_end_for_key(key_prefix),
            keys_only=keys_only,
            sort_order=sort_order,
            sort_target=sort_target,
            serializable=serializable,
            revision=revision,
        )

        return self.kvstub.Range(
            range_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

    def get_prefix(
        self,
        key_prefix: Union[str, bytes],
        keys_only: bool = False,
        sort_order: Optional[RangeSortOrder] = None,
        sort_target: Optional[RangeSortTarget] = None,
        serializable: bool = False,
        revision: int = 0,
        timeout_override: Optional[int] = None,
    ) -> Generator[KVResult, None, None]:
        """
        Get a range of keys with a prefix.

        :param key_prefix: first key in range
        :param keys_only: if True, retrieve only the keys, not the values
        :param sort_order: optionally specify result sorting; one of asc, desc, ascend, descend
        :param sort_target: what part of KV to sort on, one of key, value, version, create (revision), mod (revision)
        :param serializable: whether to allow serializable reads. This can
            result in stale reads
        :param revision: optionally specify store revision at which the get should be done; when not specified or lower or equal
         to zero, the get will be done on the latest version of the store
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        :returns: sequence of (value, metadata) tuples
        """
        range_response = self.get_prefix_response(
            key_prefix=key_prefix,
            keys_only=keys_only,
            sort_order=sort_order,
            sort_target=sort_target,
            serializable=serializable,
            revision=revision,
            timeout_override=timeout_override,
        )

        for kv in range_response.kvs:
            yield kv.value, KVMetadata.create(kv, range_response.header)

    @_handle_errors
    def get_range_response(
        self,
        range_start: Union[str, bytes],
        range_end: Union[str, bytes],
        keys_only: bool = False,
        sort_order: Optional[RangeSortOrder] = None,
        sort_target: Optional[RangeSortTarget] = None,
        serializable: bool = False,
        revision: int = 0,
        timeout_override: Optional[int] = None,
    ) -> etcdrpc.RangeResponse:
        """Get a range of keys."""
        range_request = build_get_range_request(
            key=range_start,
            range_end=range_end,
            keys_only=keys_only,
            sort_order=sort_order,
            sort_target=sort_target,
            serializable=serializable,
            revision=revision,
        )

        return self.kvstub.Range(
            range_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

    def get_range(
        self,
        range_start: Union[str, bytes],
        range_end: Union[str, bytes],
        keys_only: bool = False,
        sort_order: Optional[RangeSortOrder] = None,
        sort_target: Optional[RangeSortTarget] = None,
        serializable: bool = False,
        revision: int = 0,
        timeout_override: Optional[int] = None,
    ) -> Generator[KVResult, None, None]:
        """
        Get a range of keys.

        :param range_start: first key in range
        :param range_end: last key in range
        :param keys_only: if True, retrieve only the keys, not the values
        :param sort_order: optionally specify result sorting; one of asc, desc, ascend, descend
        :param sort_target: what part of KV to sort on, one of key, value, version, create (revision), mod (revision)
        :param serializable: whether to allow serializable reads. This can
            result in stale reads
        :param revision: optionally specify store revision at which the get should be done; when not specified or lower or equal
         to zero, the get will be done on the latest version of the store
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        :returns: sequence of (value, metadata) tuples
        """
        range_response = self.get_range_response(
            range_start=range_start,
            range_end=range_end,
            keys_only=keys_only,
            sort_order=sort_order,
            sort_target=sort_target,
            serializable=serializable,
            revision=revision,
            timeout_override=timeout_override,
        )

        for kv in range_response.kvs:
            yield kv.value, KVMetadata.create(kv, range_response.header)

    @_handle_errors
    def get_all_response(
        self,
        keys_only: bool = False,
        sort_order: Optional[RangeSortOrder] = None,
        sort_target: Optional[RangeSortTarget] = None,
        serializable: bool = False,
        revision: int = 0,
        timeout_override: Optional[int] = None,
    ) -> etcdrpc.RangeResponse:
        """Get all keys currently stored in etcd."""
        range_request = build_get_range_request(
            key=b"\0",
            range_end=b"\0",
            keys_only=keys_only,
            sort_order=sort_order,
            sort_target=sort_target,
            serializable=serializable,
            revision=revision,
        )

        return self.kvstub.Range(
            range_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

    def get_all(
        self,
        keys_only: bool = False,
        sort_order: Optional[RangeSortOrder] = None,
        sort_target: Optional[RangeSortTarget] = None,
        serializable: bool = False,
        revision: int = 0,
        timeout_override: Optional[int] = None,
    ) -> Generator[KVResult, None, None]:
        """
        Get all keys currently stored in etcd.

        :param keys_only: if True, retrieve only the keys, not the values
        :param sort_order: optionally specify result sorting; one of asc, desc, ascend, descend
        :param sort_target: what part of KV to sort on, one of key, value, version, create (revision), mod (revision)
        :param serializable: whether to allow serializable reads. This can
            result in stale reads
        :param revision: optionally specify store revision at which the get should be done; when not specified or lower or equal
         to zero, the get will be done on the latest version of the store
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        :returns: sequence of (value, metadata) tuples
        """

        range_response = self.get_all_response(
            keys_only=keys_only,
            sort_order=sort_order,
            sort_target=sort_target,
            serializable=serializable,
            revision=revision,
            timeout_override=timeout_override,
        )

        for kv in range_response.kvs:
            yield kv.value, KVMetadata.create(kv, range_response.header)

    @_handle_errors
    def put(
        self,
        key: Union[str, bytes],
        value: Union[str, bytes],
        lease: Optional[Union[int, Lease]] = None,
        prev_kv: bool = False,
        timeout_override: Optional[int] = None,
    ) -> etcdrpc.PutResponse:
        """
        Save a value to etcd.

        Example usage:

        .. code-block:: python

            >>> import etcd3
            >>> etcd = etcd3.client()
            >>> etcd.put('/thing/key', b'hello world')

        :param key: key in etcd to set
        :param value: value to set key to
        :type value: bytes
        :param lease: Lease to associate with this key.
        :type lease: either :class:`.Lease`, or int (ID of lease)
        :param prev_kv: return the previous key-value pair
        :type prev_kv: bool
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        :returns: a response containing a header and the prev_kv
        :rtype: :class:`.rpc_pb2.PutResponse`
        """
        put_request = build_put_request(key, value, lease=lease, prev_kv=prev_kv)

        return self.kvstub.Put(
            put_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

    def put_if_not_exists(
        self,
        key: Union[str, bytes],
        value: Union[str, bytes],
        lease: Optional[Union[int, Lease]] = None,
        timeout_override: Optional[int] = None,
    ) -> Tuple[bool, Optional[etcdrpc.PutResponse]]:
        """
        Atomically puts a value only if the key previously had no value.

        This operation happens in a transaction.

        On success, returns tuple of True and the response to the Put operation.
        On failure, returns False and the response is None

        :param key: key in etcd to put
        :param value: value to be written to key
        :type value: bytes
        :param lease: Lease to associate with this key.
        :type lease: either :class:`.Lease`, or int (ID of lease)
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        :returns: state of transaction, ``True`` if the put was successful,
                  ``False`` otherwise
        :rtype: bool
        """
        status, response = self.transaction(
            compare=[self.transactions.create(key) == "0"],
            success=[self.transactions.put(key, value, lease=lease)],
            failure=[],
            timeout_override=timeout_override,
        )

        if status:
            return True, cast(etcdrpc.ResponseOp, response[0]).response_put

        return False, None

    def replace(
        self,
        key: Union[str, bytes],
        initial_value: Union[str, bytes],
        new_value: Union[str, bytes],
        lease: Optional[Union[int, Lease]] = None,
        timeout_override: Optional[int] = None,
    ) -> Tuple[bool, Optional[etcdrpc.PutResponse]]:
        """
        Atomically replace the value of a key with a new value.

        This compares the current value of a key, then replaces it with a new
        value if it is equal to a specified value. This operation happens
        in a transaction.

        On success, returns tuple of True and the response to the Put operation.
        On failure, returns False and the response is None

        :param key: key in etcd to replace
        :param initial_value: old value to replace
        :type initial_value: bytes
        :param new_value: new value of the key
        :type new_value: bytes
        :param lease: Lease to associate with the key (if the replace is successful).
        :type lease: either :class:`.Lease`, or int (ID of lease)
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        :returns: status of transaction, ``True`` if the replace was successful, ``False`` otherwise
        :rtype: bool
        """
        status, response = self.transaction(
            compare=[self.transactions.value(key) == initial_value],
            success=[self.transactions.put(key, new_value, lease=lease)],
            failure=[],
            timeout_override=timeout_override,
        )

        if status:
            return True, cast(etcdrpc.ResponseOp, response[0]).response_put

        return False, None

    @_handle_errors
    def delete(
        self,
        key: Union[str, bytes],
        prev_kv: bool = False,
        timeout_override: Optional[int] = None,
    ) -> Tuple[bool, Optional[etcdrpc.DeleteRangeResponse]]:
        """
        Delete a single key in etcd.

        If the key was deleted, returns tuple of True and response of the Delete operation.
        If the key was not deleted, returns tuple of False and response is None.

        :param key: key in etcd to delete
        :param prev_kv: return the deleted key-value pair
        :type prev_kv: bool
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        :returns: True if the key has been deleted when
                  ``return_response`` is False and a response containing
                  a header, the number of deleted keys and prev_kvs when
                  ``return_response`` is True
        """
        delete_request = build_delete_request(key, prev_kv=prev_kv)
        delete_response = self.kvstub.DeleteRange(
            delete_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

        if delete_response.deleted >= 1:
            return True, delete_response

        return False, None

    @_handle_errors
    def delete_prefix(
        self,
        prefix: Union[str, bytes],
        prev_kv: bool = False,
        timeout_override: Optional[int] = None,
    ) -> etcdrpc.DeleteRangeResponse:
        """Delete a range of keys with a prefix in etcd."""
        delete_request = build_delete_request(
            prefix, range_end=range_end_for_key(prefix), prev_kv=prev_kv
        )

        return self.kvstub.DeleteRange(
            delete_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

    @_handle_errors
    def delete_range(
        self,
        key: Union[str, bytes],
        range_end: Union[str, bytes],
        prev_kv: bool = False,
        timeout_override: Optional[int] = None,
    ) -> etcdrpc.DeleteRangeResponse:
        """Delete a range of keys with a prefix in etcd."""
        delete_request = build_delete_request(key, range_end=range_end, prev_kv=prev_kv)

        return self.kvstub.DeleteRange(
            delete_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

    @_handle_errors
    def status(self, timeout_override: Optional[int] = None) -> Status:
        """Get the status of the responding member."""
        status_request = etcdrpc.StatusRequest()
        status_response = self.maintenancestub.Status(
            status_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

        for m in self.members:
            if m.id == status_response.leader:
                leader = m
                break
        else:
            leader = None

        return Status(
            status_response.version,
            status_response.dbSize,
            leader,
            status_response.raftIndex,
            status_response.raftTerm,
        )

    @_handle_errors
    def add_watch_callback(
        self,
        key: Union[str, bytes],
        callback: WatchCallback,
        range_end: Optional[Union[str, bytes]] = None,
        start_revision: Optional[int] = None,
        progress_notify: bool = False,
        filters: Optional[Iterable[WatchFilterType]] = None,
        prev_kv: bool = False,
    ) -> int:
        """
        Watch a key or range of keys and call a callback on every response.

        If timeout was declared during the client initialization and
        the watch cannot be created during that time the method raises
        a ``WatchTimedOut`` exception.

        :param key: key to watch
        :param callback: callback function
        :param range_end: end of key range if watching multiple keys
        :param start_revision: An optional revision for where to inclusively begin watching. If not given, it will
         stream events following the revision of the watch creation response header revision. The entire available
         event history can be watched starting from the last compaction revision.
        :param progress_notify: When set, the watch will periodically receive a WatchResponse with no events, if there
         are no recent events. It is useful when clients wish to recover a disconnected watcher starting from a recent
         known revision. The etcd server decides how often to send notifications based on current server load.
        :param filters: A list of event types to filter away at server side.
        :param prev_kv: When set, the watch receives the key-value data from before the event happens. This is useful
         for knowing what data has been overwritten.
        :returns: watch_id. Later it could be used for cancelling watch.
        """
        try:
            return self.watcher.add_callback(
                key=key,
                callback=callback,
                range_end=range_end,
                start_revision=start_revision,
                progress_notify=progress_notify,
                filters=filters,
                prev_kv=prev_kv,
            )
        except queue.Empty:
            raise WatchTimedOut()

    @_handle_errors
    def add_watch_prefix_callback(
        self,
        key_prefix: Union[str, bytes],
        callback: WatchCallback,
        start_revision: Optional[int] = None,
        progress_notify: bool = False,
        filters: Optional[Iterable[WatchFilterType]] = None,
        prev_kv: bool = False,
    ) -> int:
        """
        Watch a prefix and call a callback on every response.

        If timeout was declared during the client initialization and
        the watch cannot be created during that time the method raises
        a ``WatchTimedOut`` exception.

        :param key_prefix: prefix to watch
        :param callback: callback function
        :param start_revision: An optional revision for where to inclusively begin watching. If not given, it will
         stream events following the revision of the watch creation response header revision. The entire available
         event history can be watched starting from the last compaction revision.
        :param progress_notify: When set, the watch will periodically receive a WatchResponse with no events, if there
         are no recent events. It is useful when clients wish to recover a disconnected watcher starting from a recent
         known revision. The etcd server decides how often to send notifications based on current server load.
        :param filters: A list of event types to filter away at server side.
        :param prev_kv: When set, the watch receives the key-value data from before the event happens. This is useful
         for knowing what data has been overwritten.

        :returns: watch_id. Later it could be used for cancelling watch.
        """

        return self.add_watch_callback(
            key_prefix,
            callback,
            range_end=range_end_for_key(key_prefix),
            start_revision=start_revision,
            progress_notify=progress_notify,
            filters=filters,
            prev_kv=prev_kv,
        )

    @_handle_errors
    def watch_response(
        self,
        key: Union[str, bytes],
        range_end: Optional[Union[str, bytes]] = None,
        start_revision: Optional[int] = None,
        progress_notify: bool = False,
        filters: Optional[Iterable[WatchFilterType]] = None,
        prev_kv: bool = False,
    ) -> Tuple[Iterator[WatchResponse], Callable[[], Any]]:
        """
        Watch a key.

        Example usage:

        .. code-block:: python
            responses_iterator, cancel = etcd.watch_response('/doot/key')
            for response in responses_iterator:
                print(response)

        :param key: key to watch
        :param range_end: end of key range if watching multiple keys
        :param start_revision: An optional revision for where to inclusively begin watching. If not given, it will
         stream events following the revision of the watch creation response header revision. The entire available
         event history can be watched starting from the last compaction revision.
        :param progress_notify: When set, the watch will periodically receive a WatchResponse with no events, if there
         are no recent events. It is useful when clients wish to recover a disconnected watcher starting from a recent
         known revision. The etcd server decides how often to send notifications based on current server load.
        :param filters: A list of event types to filter away at server side.
        :param prev_kv: When set, the watch receives the key-value data from before the event happens. This is useful
         for knowing what data has been overwritten.

        :returns: tuple of ``responses_iterator`` and ``cancel``.
                  Use ``responses_iterator`` to get the watch responses,
                  each of which contains a header and a list of events.
                  Use ``cancel`` to cancel the watch request.
        """
        response_queue: queue.Queue[
            Union[Exception, WatchResponse, None]
        ] = queue.Queue()

        watch_id = self.add_watch_callback(
            key,
            response_queue.put,
            range_end=range_end,
            start_revision=start_revision,
            progress_notify=progress_notify,
            filters=filters,
            prev_kv=prev_kv,
        )
        canceled = threading.Event()

        def cancel() -> None:
            canceled.set()
            response_queue.put(None)
            self.cancel_watch(watch_id)

        @_handle_errors
        def iterator() -> Iterator[WatchResponse]:
            while not canceled.is_set():
                response = response_queue.get()

                if response is None:
                    canceled.set()
                elif isinstance(response, Exception):
                    canceled.set()
                    raise response
                elif not canceled.is_set():
                    yield response

        return iterator(), cancel

    def watch(
        self,
        key: Union[str, bytes],
        range_end: Optional[Union[str, bytes]] = None,
        start_revision: Optional[int] = None,
        progress_notify: bool = False,
        filters: Optional[Iterable[WatchFilterType]] = None,
        prev_kv: bool = False,
    ) -> Tuple[Iterator[Event], Callable[[], Any]]:
        """
        Watch a key.

        Example usage:

        .. code-block:: python
            events_iterator, cancel = etcd.watch('/doot/key')
            for event in events_iterator:
                print(event)

        :param key: key to watch
        :param range_end: end of key range if watching multiple keys
        :param start_revision: An optional revision for where to inclusively begin watching. If not given, it will
         stream events following the revision of the watch creation response header revision. The entire available
         event history can be watched starting from the last compaction revision.
        :param progress_notify: When set, the watch will periodically receive a WatchResponse with no events, if there
         are no recent events. It is useful when clients wish to recover a disconnected watcher starting from a recent
         known revision. The etcd server decides how often to send notifications based on current server load.
        :param filters: A list of event types to filter away at server side.
        :param prev_kv: When set, the watch receives the key-value data from before the event happens. This is useful
         for knowing what data has been overwritten.

        :returns: tuple of ``events_iterator`` and ``cancel``.
                  Use ``events_iterator`` to get the events of key changes
                  and ``cancel`` to cancel the watch request.
        """
        response_iterator, cancel = self.watch_response(
            key,
            range_end=range_end,
            start_revision=start_revision,
            progress_notify=progress_notify,
            filters=filters,
            prev_kv=prev_kv,
        )

        def _to_event_iterator(
            iterator: Iterator[WatchResponse],
        ) -> Iterator[Event]:
            for response in iterator:
                for event in response.events:
                    yield event

        return _to_event_iterator(response_iterator), cancel

    def watch_prefix_response(
        self,
        key_prefix: Union[str, bytes],
        start_revision: Optional[int] = None,
        progress_notify: bool = False,
        filters: Optional[Iterable[WatchFilterType]] = None,
        prev_kv: bool = False,
    ) -> Tuple[Iterator[WatchResponse], Callable[[], Any]]:
        """
        Watch a range of keys with a prefix.

        :param key_prefix: prefix to watch
        :param start_revision: An optional revision for where to inclusively begin watching. If not given, it will
         stream events following the revision of the watch creation response header revision. The entire available
         event history can be watched starting from the last compaction revision.
        :param progress_notify: When set, the watch will periodically receive a WatchResponse with no events, if there
         are no recent events. It is useful when clients wish to recover a disconnected watcher starting from a recent
         known revision. The etcd server decides how often to send notifications based on current server load.
        :param filters: A list of event types to filter away at server side.
        :param prev_kv: When set, the watch receives the key-value data from before the event happens. This is useful
         for knowing what data has been overwritten.

        :returns: tuple of ``responses_iterator`` and ``cancel``.
        """

        return self.watch_response(
            key_prefix,
            range_end=range_end_for_key(key_prefix),
            start_revision=start_revision,
            progress_notify=progress_notify,
            filters=filters,
            prev_kv=prev_kv,
        )

    def watch_prefix(
        self,
        key_prefix: Union[str, bytes],
        start_revision: Optional[int] = None,
        progress_notify: bool = False,
        filters: Optional[Iterable[WatchFilterType]] = None,
        prev_kv: bool = False,
    ) -> Tuple[Iterator[Event], Callable[[], Any]]:
        """
        Watch a range of keys with a prefix.

        :param key_prefix: prefix to watch
        :param start_revision: An optional revision for where to inclusively begin watching. If not given, it will
         stream events following the revision of the watch creation response header revision. The entire available
         event history can be watched starting from the last compaction revision.
        :param progress_notify: When set, the watch will periodically receive a WatchResponse with no events, if there
         are no recent events. It is useful when clients wish to recover a disconnected watcher starting from a recent
         known revision. The etcd server decides how often to send notifications based on current server load.
        :param filters: A list of event types to filter away at server side.
        :param prev_kv: When set, the watch receives the key-value data from before the event happens. This is useful
         for knowing what data has been overwritten.

        :returns: tuple of ``events_iterator`` and ``cancel``.
        """

        return self.watch(
            key_prefix,
            range_end=range_end_for_key(key_prefix),
            start_revision=start_revision,
            progress_notify=progress_notify,
            filters=filters,
            prev_kv=prev_kv,
        )

    @_handle_errors
    def watch_once_response(
        self,
        key: Union[str, bytes],
        timeout: Optional[float] = None,
        range_end: Optional[Union[str, bytes]] = None,
        start_revision: Optional[int] = None,
        progress_notify: bool = False,
        filters: Optional[Iterable[WatchFilterType]] = None,
        prev_kv: bool = False,
    ) -> WatchResponse:
        """
        Watch a key and stop after the first response.

        If the timeout was specified and response didn't arrive method
        will raise ``WatchTimedOut`` exception.

        :param key: key to watch
        :param timeout: (optional) timeout in seconds.
        :param range_end: end of key range if watching multiple keys
        :param start_revision: An optional revision for where to inclusively begin watching. If not given, it will
         stream events following the revision of the watch creation response header revision. The entire available
         event history can be watched starting from the last compaction revision.
        :param progress_notify: When set, the watch will periodically receive a WatchResponse with no events, if there
         are no recent events. It is useful when clients wish to recover a disconnected watcher starting from a recent
         known revision. The etcd server decides how often to send notifications based on current server load.
        :param filters: A list of event types to filter away at server side.
        :param prev_kv: When set, the watch receives the key-value data from before the event happens. This is useful
         for knowing what data has been overwritten.

        :returns: ``WatchResponse``
        """
        response_queue: queue.Queue[Union[Exception, WatchResponse]] = queue.Queue()
        watch_id = self.add_watch_callback(
            key,
            response_queue.put,
            range_end=range_end,
            start_revision=start_revision,
            progress_notify=progress_notify,
            filters=filters,
            prev_kv=prev_kv,
        )

        try:
            res = response_queue.get(timeout=timeout)

            if isinstance(res, Exception):
                raise Exception

            return res
        except queue.Empty:
            raise WatchTimedOut()
        finally:
            self.cancel_watch(watch_id)

    def watch_once(
        self,
        key: Union[str, bytes],
        timeout: Optional[float] = None,
        range_end: Optional[Union[str, bytes]] = None,
        start_revision: Optional[int] = None,
        progress_notify: bool = False,
        filters: Optional[Iterable[WatchFilterType]] = None,
        prev_kv: bool = False,
    ) -> List[Event]:
        """
        Watch a key and stop after the first batch of events.

        If the timeout was specified and event didn't arrive method
        will raise ``WatchTimedOut`` exception.

        BREAKING: in 0.12.0, this was returning first event of possibly multiple events that describe
        a transaction done on watched keys. It now returns a list of events.

        :param key: key to watch
        :param timeout: (optional) timeout in seconds.
        :param range_end: end of key range if watching multiple keys
        :param start_revision: An optional revision for where to inclusively begin watching. If not given, it will
         stream events following the revision of the watch creation response header revision. The entire available
         event history can be watched starting from the last compaction revision.
        :param progress_notify: When set, the watch will periodically receive a WatchResponse with no events, if there
         are no recent events. It is useful when clients wish to recover a disconnected watcher starting from a recent
         known revision. The etcd server decides how often to send notifications based on current server load.
        :param filters: A list of event types to filter away at server side.
        :param prev_kv: When set, the watch receives the key-value data from before the event happens. This is useful
         for knowing what data has been overwritten.

        :returns: ``Event``
        """
        response = self.watch_once_response(
            key,
            timeout=timeout,
            range_end=range_end,
            start_revision=start_revision,
            progress_notify=progress_notify,
            filters=filters,
            prev_kv=prev_kv,
        )

        return response.events

    def watch_prefix_once_response(
        self,
        key_prefix: Union[str, bytes],
        timeout: Optional[float] = None,
        start_revision: Optional[int] = None,
        progress_notify: bool = False,
        filters: Optional[Iterable[WatchFilterType]] = None,
        prev_kv: bool = False,
    ) -> WatchResponse:
        """
        Watch a range of keys with a prefix and stop after the first response.

        If the timeout was specified and response didn't arrive method
        will raise ``WatchTimedOut`` exception.
        """

        return self.watch_once_response(
            key_prefix,
            timeout=timeout,
            range_end=range_end_for_key(key_prefix),
            start_revision=start_revision,
            progress_notify=progress_notify,
            filters=filters,
            prev_kv=prev_kv,
        )

    def watch_prefix_once(
        self,
        key_prefix: Union[str, bytes],
        timeout: Optional[float] = None,
        start_revision: Optional[int] = None,
        progress_notify: bool = False,
        filters: Optional[Iterable[WatchFilterType]] = None,
        prev_kv: bool = False,
    ) -> List[Event]:
        """
        Watch a range of keys with a prefix and stop after the first event.

        If the timeout was specified and event didn't arrive method
        will raise ``WatchTimedOut`` exception.
        """
        return self.watch_once(
            key_prefix,
            timeout=timeout,
            range_end=range_end_for_key(key_prefix),
            start_revision=start_revision,
            progress_notify=progress_notify,
            filters=filters,
            prev_kv=prev_kv,
        )

    @_handle_errors
    def cancel_watch(self, watch_id: int) -> None:
        """
        Stop watching a key or range of keys.

        :param watch_id: watch_id returned by ``add_watch_callback`` method
        """
        self.watcher.cancel(watch_id)

    @_handle_errors
    def transaction(
        self,
        compare: Iterable[TxCondition],
        success: Optional[Iterable[TxOp]] = None,
        failure: Optional[Iterable[TxOp]] = None,
        timeout_override: Optional[int] = None,
    ) -> TxResult:
        """
        Perform a transaction.

        Example usage:

        .. code-block:: python

            etcd.transaction(
                compare=[
                    etcd.transactions.value('/doot/testing') == 'doot',
                    etcd.transactions.version('/doot/testing') > 0,
                ],
                success=[
                    etcd.transactions.put('/doot/testing', 'success'),
                ],
                failure=[
                    etcd.transactions.put('/doot/testing', 'failure'),
                ]
            )

        :param compare: A list of comparisons to make
        :param success: A list of operations to perform if all the comparisons
                        are true
        :param failure: A list of operations to perform if any of the
                        comparisons are false
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        :return: A tuple of (operation status, responses)
        """

        transaction_request = build_tx_request(compare, success, failure)

        txn_response: etcdrpc.TxnResponse = self.kvstub.Txn(
            transaction_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

        responses: List[TxResponse] = []
        for response in txn_response.responses:
            response_type = response.WhichOneof("response")
            if response_type in [
                "response_put",
                "response_delete_range",
                "response_txn",
            ]:
                responses.append(response)

            elif response_type == "response_range":
                range_kvs = []
                for kv in response.response_range.kvs:
                    range_kvs.append(
                        (kv.value, KVMetadata.create(kv, txn_response.header))
                    )

                responses.append(range_kvs)

        return txn_response.succeeded, responses

    @_handle_errors
    def txn(
        self, tx: TransactionBuilder, timeout_override: Optional[int] = None
    ) -> TransactionResponse:
        """
        Perform a transaction as defined in the `TransactionBuilder`.

        The TransactionBuilder is a convenience that helps you build the lists that contain compares, success and
        failure operations. It has fluent API and can possibly for in cumulative (immutable) fashion.

        The response is wrapped in the `TransactionResponse` facade that allows for convenient and
        typed access to the results.

        :param tx: transaction builder
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        :return: transaction response
        """
        request = tx.request
        response: etcdrpc.TxnResponse = self.kvstub.Txn(
            request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

        return TransactionResponse(response)

    @_handle_errors
    def lease(
        self,
        ttl: int,
        lease_id: Optional[int] = None,
        timeout_override: Optional[int] = None,
    ) -> Lease:
        """
        Create a new lease.

        All keys attached to this lease will be expired and deleted if the
        lease expires. A lease can be sent keep alive messages to refresh the
        ttl.

        :param ttl: Requested time to live
        :param lease_id: Requested ID for the lease
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        :returns: new lease
        :rtype: :class:`.Lease`
        """
        lease_grant_request = etcdrpc.LeaseGrantRequest(TTL=ttl, ID=lease_id or 0)
        lease_grant_response = self.leasestub.LeaseGrant(
            lease_grant_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )
        return Lease(
            lease_id=lease_grant_response.ID,
            ttl=lease_grant_response.TTL,
            get_lease_info=self.get_lease_info,
            revoke_lease=self.revoke_lease,
            refresh_lease=self.refresh_lease,
        )

    @_handle_errors
    def revoke_lease(
        self, lease_id: int, timeout_override: Optional[int] = None
    ) -> None:
        """
        Revoke a lease.

        :param lease_id: ID of the lease to revoke.
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        """
        lease_revoke_request = etcdrpc.LeaseRevokeRequest(ID=lease_id)
        self.leasestub.LeaseRevoke(
            lease_revoke_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

    @_handle_errors
    def refresh_lease(
        self, lease_id: int, timeout_override: Optional[int] = None
    ) -> Generator[etcdrpc.LeaseKeepAliveResponse, None, None]:
        """
        Refreshes the lease. This returns a generator that will yield exactly once and will provide the
        response to the refresh request.

        Note: if the refresh failed, the TTL field of the response will be 0.

        :param lease_id: lease id to refresh
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        :return: generator that yields one refresh response and then ends
        """
        keep_alive_request = etcdrpc.LeaseKeepAliveRequest(ID=lease_id)
        request_stream = [keep_alive_request]

        for response in self.leasestub.LeaseKeepAlive(
            iter(request_stream),
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        ):
            yield response

    @_handle_errors
    def get_lease_info(
        self, lease_id: int, keys: bool = True, timeout_override: Optional[int] = None
    ) -> etcdrpc.LeaseTimeToLiveResponse:
        # only available in etcd v3.1.0 and later
        ttl_request = etcdrpc.LeaseTimeToLiveRequest(ID=lease_id, keys=keys)

        return self.leasestub.LeaseTimeToLive(
            ttl_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

    @_handle_errors
    def add_member(
        self, urls: Iterable[str], timeout_override: Optional[int] = None
    ) -> Member:
        """
        Add a member into the cluster.

        :param urls: peer urls
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        :returns: new member
        :rtype: :class:`.Member`
        """
        member_add_request = etcdrpc.MemberAddRequest(peerURLs=list(urls))

        member_add_response: etcdrpc.MemberAddResponse = self.clusterstub.MemberAdd(
            member_add_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

        member = member_add_response.member

        return Member(
            id=member.ID,
            name=member.name,
            peer_urls=list(member.peerURLs),
            client_urls=list(member.clientURLs),
        )

    @_handle_errors
    def remove_member(
        self, member_id: int, timeout_override: Optional[int] = None
    ) -> None:
        """
        Remove an existing member from the cluster.

        :param member_id: ID of the member to remove
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        """
        member_rm_request = etcdrpc.MemberRemoveRequest(ID=member_id)

        self.clusterstub.MemberRemove(
            member_rm_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

    @_handle_errors
    def update_member(
        self,
        member_id: int,
        peer_urls: Iterable[str],
        timeout_override: Optional[int] = None,
    ) -> None:
        """
        Update the configuration of an existing member in the cluster.

        :param member_id: ID of the member to update
        :param peer_urls: new list of peer urls the member will use to
                          communicate with the cluster
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        """
        member_update_request = etcdrpc.MemberUpdateRequest(
            ID=member_id, peerURLs=list(peer_urls)
        )

        self.clusterstub.MemberUpdate(
            member_update_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

    @property
    def members(
        self, timeout_override: Optional[int] = None
    ) -> Generator[Member, None, None]:
        """
        List of all members associated with the cluster.

        :type: sequence of :class:`.Member`
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        """
        member_list_request = etcdrpc.MemberListRequest()
        member_list_response: etcdrpc.MemberListResponse = self.clusterstub.MemberList(
            member_list_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

        for member in member_list_response.members:
            yield Member(
                id=member.ID,
                name=member.name,
                peer_urls=list(member.peerURLs),
                client_urls=list(member.clientURLs),
            )

    @_handle_errors
    def compact(
        self,
        revision: int,
        physical: bool = False,
        timeout_override: Optional[int] = None,
    ) -> None:
        """
        Compact the event history in etcd up to a given revision.

        All superseded keys with a revision less than the compaction revision
        will be removed.

        :param revision: revision for the compaction operation
        :param physical: if set to True, the request will wait until the
                         compaction is physically applied to the local database
                         such that compacted entries are totally removed from
                         the backend database
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        """
        compact_request = etcdrpc.CompactionRequest(
            revision=revision, physical=physical
        )
        self.kvstub.Compact(
            compact_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

    @_handle_errors
    def defragment(
        self, timeout_override: Optional[int] = None
    ) -> etcdrpc.DefragmentResponse:
        """Defragment a member's backend database to recover storage space."""
        defrag_request = etcdrpc.DefragmentRequest()

        return self.maintenancestub.Defragment(
            defrag_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

    @_handle_errors
    def hash(self) -> int:
        """
        Return the hash of the local KV state.

        :returns: kv state hash
        :rtype: int
        """
        hash_request = etcdrpc.HashRequest()
        return self.maintenancestub.Hash(hash_request).hash

    @_handle_errors
    def create_alarm(
        self,
        member_id: int = 0,
        alarm_type: AlarmType = "no space",
        timeout_override: Optional[int] = None,
    ) -> List[Alarm]:
        """Create an alarm.

        If no member id is given, the alarm is activated for all the
        members of the cluster. Only the `no space` alarm can be raised.

        :param member_id: The cluster member id to create an alarm to.
                          If 0, the alarm is created for all the members
                          of the cluster.
        :param alarm_type: type of alarm to create
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        :returns: list of :class:`.Alarm`
        """
        alarm_request = build_alarm_request(
            alarm_action=etcdrpc.AlarmRequest.ACTIVATE,
            member_id=member_id,
            alarm_type=alarm_type,
        )

        alarm_response: etcdrpc.AlarmResponse = self.maintenancestub.Alarm(
            alarm_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

        return [Alarm(alarm.alarm, alarm.memberID) for alarm in alarm_response.alarms]

    @_handle_errors
    def list_alarms(
        self,
        member_id: int = 0,
        alarm_type: AlarmType = "none",
        timeout_override: Optional[int] = None,
    ) -> Generator[Alarm, None, None]:
        """List the activated alarms.

        :param member_id: The cluster member id to create an alarm to.
                           If 0, the alarm is created for all the members
                           of the cluster.
        :param alarm_type: type of alarm to list
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        :returns: sequence of :class:`.Alarm`
        """
        alarm_request = build_alarm_request(
            alarm_action=etcdrpc.AlarmRequest.GET,
            member_id=member_id,
            alarm_type=alarm_type,
        )
        alarm_response = self.maintenancestub.Alarm(
            alarm_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

        for alarm in alarm_response.alarms:
            yield Alarm(alarm.alarm, alarm.memberID)

    @_handle_errors
    def disarm_alarm(
        self,
        member_id: int = 0,
        alarm_type: AlarmType = "no space",
        timeout_override: Optional[int] = None,
    ) -> List[Alarm]:
        """Cancel an alarm.

        :param member_id: The cluster member id to cancel an alarm.
                          If 0, the alarm is canceled for all the members
                          of the cluster.
        :param alarm_type: type of alarm to disarm
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        :returns: List of :class:`.Alarm`
        """
        alarm_request = build_alarm_request(
            alarm_action=etcdrpc.AlarmRequest.DEACTIVATE,
            member_id=member_id,
            alarm_type=alarm_type,
        )
        alarm_response = self.maintenancestub.Alarm(
            alarm_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

        return [Alarm(alarm.alarm, alarm.memberID) for alarm in alarm_response.alarms]

    @_handle_errors
    def snapshot(
        self, file_obj: BinaryIO, timeout_override: Optional[int] = None
    ) -> None:
        """Take a snapshot of the database.

        :param file_obj: A file-like object to write the database contents in.
        :param timeout_override: optionally specify timeout for just this one call; if not
         specified, the global timeout set during client creation will be used
        """
        snapshot_request = etcdrpc.SnapshotRequest()
        snapshot_response = self.maintenancestub.Snapshot(
            snapshot_request,
            timeout_override or self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata,
        )

        for response in snapshot_response:
            file_obj.write(response.blob)


def client(
    host: str = "localhost",
    port: int = 2379,
    ca_cert: Optional[str] = None,
    cert_key: Optional[str] = None,
    cert_cert: Optional[str] = None,
    timeout: Optional[int | float] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    grpc_options: Optional[List[Tuple[str, Any]]] = None,
) -> Etcd3Client:
    """Return an instance of an Etcd3Client."""
    return Etcd3Client(
        host=host,
        port=port,
        ca_cert=ca_cert,
        cert_key=cert_key,
        cert_cert=cert_cert,
        timeout=timeout,
        user=user,
        password=password,
        grpc_options=grpc_options,
    )
