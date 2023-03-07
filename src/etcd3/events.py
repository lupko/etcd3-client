import etcd3.rpc as etcdrpc
from etcd3.base import KVMetadata


class Event:
    """
    Base type for etcd event wrapper. The purpose of the wrapper is to provide additional convenience on top of
    the raw events received via etcd watches.
    """

    __slots__ = ("_value", "_kv_meta", "_prev_value", "_prev_kv_meta", "_event")

    def __init__(self, event: etcdrpc.Event, header: etcdrpc.ResponseHeader):
        self._value = event.kv.value
        self._kv_meta = KVMetadata.create(kv=event.kv, header=header)

        self._prev_value = event.prev_kv.value
        self._prev_kv_meta = KVMetadata.create(kv=event.prev_kv, header=header)

        self._event = event

    #
    # getters for the 'core' stuff
    #

    @property
    def key(self) -> bytes:
        """
        :return: key to which the event pertains
        """
        return self._kv_meta.key

    @property
    def value(self) -> bytes:
        """
        :return: value of the key (always b"" if delete event)
        """
        return self._value

    @property
    def kv_meta(self) -> KVMetadata:
        """
        :return: key-value metadata
        """
        return self._kv_meta

    @property
    def prev_value(self) -> bytes:
        """
        :return: value of the previous version of the key (in delete events, this is the value that got deleted)
        """
        return self._prev_value

    @property
    def prev_kv_meta(self) -> KVMetadata:
        """
        :return: key-value metadata for the previous version of the key
        """
        return self._prev_kv_meta

    @property
    def event(self) -> etcdrpc.Event:
        """
        :return: the raw event received from etcd
        """
        return self._event

    @property
    def header(self) -> etcdrpc.ResponseHeader:
        """
        :return: response header of the watch response that included this event
        """
        return self._kv_meta.response_header

    #
    # convenience, delegates, things to keep backward compatibility
    #

    @property
    def create_revision(self) -> int:
        return self._kv_meta.create_revision

    @property
    def mod_revision(self) -> int:
        return self._kv_meta.mod_revision

    @property
    def version(self) -> int:
        return self._kv_meta.version

    @property
    def lease(self) -> int:
        return self._kv_meta.lease_id

    @property
    def prev_key(self) -> bytes:
        return self._prev_kv_meta.key

    @property
    def prev_create_revision(self) -> int:
        return self._prev_kv_meta.create_revision

    @property
    def prev_mod_revision(self) -> int:
        return self._prev_kv_meta.mod_revision

    @property
    def prev_version(self) -> int:
        return self._prev_kv_meta.version

    @property
    def prev_lease(self) -> int:
        return self._prev_kv_meta.lease_id

    @staticmethod
    def create_event(event: etcdrpc.Event, header: etcdrpc.ResponseHeader) -> "Event":
        """
        Creates concrete subtype of the event
        :param event:
        :param header:
        :return:
        """
        if event.type == etcdrpc.Event.PUT:
            return PutEvent(event=event, header=header)
        elif event.type == etcdrpc.Event.DELETE:
            return DeleteEvent(event=event, header=header)
        else:
            raise AssertionError("Unexpected event type")

    def __str__(self) -> str:
        return f"{self.__class__} key={self.key!r} value={self.value!r}"


class PutEvent(Event):
    """
    This event is emitted when a PUT is done on some key in etcd.
    """

    __slots__ = ()


class DeleteEvent(Event):
    """
    This event is emitted when some key in etcd is DELETED.
    """

    __slots__ = ()
