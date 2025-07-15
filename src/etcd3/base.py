from dataclasses import dataclass
from typing import List, Optional, Tuple, Union

from typing_extensions import TypeAlias

from etcd3 import rpc as etcdrpc


class KVMetadata:
    """
    Convenience wrapper for etcd KeyValue metadata
    """

    __slots__ = (
        "_kv",
        "_header",
    )

    def __init__(self, kv: etcdrpc.KeyValue, header: etcdrpc.ResponseHeader):
        self._kv = kv
        self._header = header

    @property
    def key(self) -> bytes:
        """
        :return: key name
        """
        return self._kv.key

    @property
    def create_revision(self) -> int:
        """
        :return: revision at which the key was created
        """
        return self._kv.create_revision

    @property
    def mod_revision(self) -> int:
        """
        :return: revision at which the key was modified
        """
        return self._kv.mod_revision

    @property
    def version(self) -> int:
        """
        :return: key's version (number of times the key was modified)
        """
        return self._kv.version

    @property
    def lease_id(self) -> int:
        """
        :return: lease associated with this key; 0 if no lease associated
        """
        return self._kv.lease

    @property
    def response_header(self) -> etcdrpc.ResponseHeader:
        """
        :return: header of etcd response which included the KeyValue information (this may be response for the
         get call or when KVMetadata comes from watches it will be header included in watch response)
        """
        return self._header

    @staticmethod
    def create(kv: etcdrpc.KeyValue, header: etcdrpc.ResponseHeader) -> "KVMetadata":
        """
        Factory creates KVMetadata instance using etcd's KeyValue and ResponseHeader.

        :return: always new instance of KVMetadata
        """
        return KVMetadata(kv=kv, header=header)


@dataclass(frozen=True)
class Member:
    """
    A member of the etcd cluster
    """

    id: int
    """members identifier"""

    name: str
    """member name"""

    peer_urls: List[str]
    """list of URLs the member exposes to the cluster for communication"""

    client_urls: List[str]
    """list of URLs the member exposes to the clients for communication"""


@dataclass(frozen=True)
class Status:
    """etcd cluster status"""

    cluster_id: int
    """cluster_id is the ID of the cluster which sent the response."""

    member_id: int
    """member_id is the ID of the member which sent the response."""

    version: str
    """version"""

    storage_version: str
    "version of the db file. It might be get updated with delay in relationship to the target cluster version." ""

    db_size: int
    """database size"""

    db_size_in_use: int
    """size of the backend database logically in use, in bytes, of the responding member."""

    leader_id: int
    """leader_id is the member ID which the responding member believes is the current leader."""

    leader: Optional[Member]
    """leader member; None if cluster has no leader"""

    raft_index: int
    """raft index"""

    raft_applied_index: int
    """current raft applied index of the responding member."""

    raft_term: int
    """raft term"""

    is_learner: bool
    """indicates if the member is raft learner."""


@dataclass(frozen=True)
class Alarm:
    alarm_type: etcdrpc.AlarmType.ValueType
    """
    type of alarm
    """

    member_id: int
    """member identifier; may be 0 if alarm set on all members"""


KVResult: TypeAlias = Tuple[bytes, KVMetadata]
"""
KeyValue pairs from etcd are converted to this convenient form.
"""

GetResult: TypeAlias = List[KVResult]
"""
Result of get (range) operation is one or more KeyValue pairs.
"""

TxResponse: TypeAlias = Union[etcdrpc.ResponseOp, GetResult]
"""
Response to a single operation within transaction. Either 'raw' put, delete or txn operation response or for get
a conveniently transformed KeyValues.
"""

TxResponses: TypeAlias = List[TxResponse]
"""
Transaction responses. This is a list of per-operation responses.
"""

TxResult: TypeAlias = Tuple[bool, TxResponses]
"""
Transaction result. First element indicates success vs failure. Second element is per-operation responses. If
transaction was successful, then responses are for the 'success' operations. If transaction failed, then responses
are for the 'failure' operations.
"""
