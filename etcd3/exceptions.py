class Etcd3Exception(Exception):
    pass


class WatchTimedOut(Etcd3Exception):
    pass


class InternalServerError(Etcd3Exception):
    pass


class ConnectionFailedError(Etcd3Exception):
    def __str__(self) -> str:
        return "etcd connection failed"


class ConnectionTimeoutError(Etcd3Exception):
    def __str__(self) -> str:
        return "etcd connection timeout"


class PreconditionFailedError(Etcd3Exception):
    pass


class RevisionCompactedError(Etcd3Exception):
    def __init__(self, compacted_revision: int):
        super(RevisionCompactedError, self).__init__()

        self.compacted_revision = compacted_revision
