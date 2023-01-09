from dataclasses import dataclass
from typing import List, Optional, Tuple, Union

from typing_extensions import TypeAlias

from etcd3 import rpc as etcdrpc


class KVMetadata:
    """
    Convenience wrapper for etcd KeyValue metadata
    """

    __slots__ = (
        "_key",
        "_create_revision",
        "_mod_revision",
        "_version",
        "_lease_id",
        "_response_header",
    )

    def __init__(
        self,
        key: bytes,
        create_revision: int,
        mod_revision: int,
        version: int,
        lease_id: int,
        response_header: etcdrpc.ResponseHeader,
    ):
        self._key: bytes = key
        self._create_revision: int = create_revision
        self._mod_revision: int = mod_revision
        self._version: int = version
        self._lease_id: int = lease_id
        self._response_header: etcdrpc.ResponseHeader = response_header

    @property
    def key(self) -> bytes:
        """
        :return: key name
        """
        return self._key

    @property
    def create_revision(self) -> int:
        """
        :return: revision at which the key was created
        """
        return self._create_revision

    @property
    def mod_revision(self) -> int:
        """
        :return: revision at which the key was modified
        """
        return self._mod_revision

    @property
    def version(self) -> int:
        """
        :return: key's version (number of times the key was modified)
        """
        return self._version

    @property
    def lease_id(self) -> int:
        """
        :return: lease associated with this key; 0 if no lease associated
        """
        return self._lease_id

    @property
    def response_header(self) -> etcdrpc.ResponseHeader:
        """
        :return: header of etcd response which included the KeyValue information (this may be response for the
         get call or when KVMetadata comes from watches it will be header included in watch response)
        """
        return self._response_header

    @staticmethod
    def create(kv: etcdrpc.KeyValue, header: etcdrpc.ResponseHeader) -> "KVMetadata":
        """
        Factory creates KVMetadata instance using etcd's KeyValue and ResponseHeader.

        :return: always new instance of KVMetadata
        """
        return KVMetadata(
            key=kv.key,
            create_revision=kv.create_revision,
            mod_revision=kv.mod_revision,
            version=kv.version,
            lease_id=kv.lease,
            response_header=header,
        )


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

    version: str
    """version"""

    db_size: int
    """database size"""

    leader: Optional[Member]
    """leader member; None if cluster has no leader"""

    raft_index: int
    """raft index"""

    raft_term: int
    """raft term"""


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
