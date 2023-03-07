from typing import Any, Callable, Generator, Tuple

from typing_extensions import TypeAlias

import etcd3.rpc as etcdrpc

GetLeaseInfoFun: TypeAlias = Callable[[int, bool], etcdrpc.LeaseTimeToLiveResponse]
RevokeLeaseFun: TypeAlias = Callable[[int], Any]
RefreshLeaseFun: TypeAlias = Callable[
    [int], Generator[etcdrpc.LeaseKeepAliveResponse, None, None]
]


class Lease:
    """
    Represents an etcd Lease. This is a 'live' object - its properties and methods talk directly to
    etcd to obtain current status of the Lease from etcd.
    """

    def __init__(
        self,
        lease_id: int,
        ttl: int,
        get_lease_info: GetLeaseInfoFun,
        revoke_lease: RevokeLeaseFun,
        refresh_lease: RefreshLeaseFun,
    ):
        self._id = lease_id
        self._ttl = ttl

        self._get_lease_info = get_lease_info
        self._revoke_lease = revoke_lease
        self._refresh_lease = refresh_lease

    @property
    def id(self) -> int:
        """
        :return: lease identifier
        """
        return self._id

    @property
    def ttl(self) -> int:
        """
        :return: TTL that was used to create the lease; seconds
        """
        return self._ttl

    @property
    def remaining_ttl(self) -> int:
        """
        :return: TTL remaining for the lease; seconds
         NOTE: this will get the latest info from etcd
        """
        return self._get_lease_info(self._id, False).TTL

    @property
    def granted_ttl(self) -> int:
        """
        :return: TTL that was granted by etcd upon creation or refresh of the lease; this is the initial TTL value;
         NOTE: this will get the latest info from etcd
        """
        return self._get_lease_info(self._id, False).grantedTTL

    @property
    def keys(self) -> Tuple[bytes, ...]:
        """
        :return: keys associated with the lease;
         NOTE: this will get the latest set of keys from etcd
        """
        return tuple(self._get_lease_info(self._id, True).keys)

    def revoke(self) -> None:
        """Revoke this lease."""
        self._revoke_lease(self._id)

    def refresh(self) -> etcdrpc.LeaseKeepAliveResponse:
        """Refresh the time to live for this lease."""

        for refresh in self._refresh_lease(self._id):
            return refresh

        raise AssertionError(
            "Lease refresh yielded no result. "
            "It should either raise error or yield exactly one result."
        )
