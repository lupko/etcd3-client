import logging
import queue
import threading
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

import grpc
from typing_extensions import TypeAlias

import etcd3.rpc as etcdrpc
from etcd3.events import Event
from etcd3.exceptions import RevisionCompactedError, WatchTimedOut
from etcd3.request_factory import WatchFilterType, build_create_watch_request

_log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class WatchResponse:
    """
    Response of watch operation.

    This object is index-able and iterable. You can access events by their index, or you can iterate
    through all events using a generator.

    Alternatively, you can materialize all events into a list and work with it - but this is kept here
    only for backward compatibility and its use is highly discouraged.
    """

    watch_response: etcdrpc.WatchResponse

    @property
    def header(self) -> etcdrpc.ResponseHeader:
        """
        etcd response header attached to the WatchResponse.

        IMPORTANT: do not use header.revision for purposes of tracking
        revision high watermark.

        :return: response header
        """
        return self.watch_response.header

    @property
    def fragment(self) -> bool:
        """
        :return: true if this watch response is a fragment
        """
        return self.watch_response.fragment

    @property
    def events(self) -> List[Event]:
        """
        Gets all events in a list.

        IMPORTANT: this will read and materialize all events. When working with large watch responses,
        this method will lead to increased memory consumption. If this is a concern for you, consider
        iterating this response which uses generators.

        :return: all events in a list
        """
        return list(self.all())

    @property
    def event_counts(self) -> tuple[int, int]:
        """
        Counts number of PUT and DELETE event types found in the watch response.

        Note: this does not materialize the event classes.

        :return: tuple of (number_of_put_events, number_of_delete_events)
        """
        put_events = 0
        delete_events = 0

        for event in self.watch_response.events:
            if event.type == etcdrpc.Event.PUT:
                put_events += 1
            elif event.type == etcdrpc.Event.DELETE:
                delete_events += 1

        return put_events, delete_events

    @property
    def max_mod_revision(self) -> Optional[int]:
        """
        Gets maximum mod revision found in the events included in this
        watch response. This method will not materialize event classes.

        HINT: this is suitable for tracking revision high watermark.


        :return: None if the watch response is empty
        """
        result = -1

        for event in self.watch_response.events:
            if event.kv.mod_revision > result:
                result = event.kv.mod_revision

        return result if result > -1 else None

    def all(self) -> Generator[Event, None, None]:
        """
        Generator of events included in the watch response.

        :return: generator yielding instances of `Event`
        """
        header = self.watch_response.header

        for event in self.watch_response.events:
            yield Event.create_event(event=event, header=header)

    def __iter__(self) -> Generator[Event, None, None]:
        yield from self.all()

    def __getitem__(self, index: Any) -> Event:
        return Event.create_event(
            event=self.watch_response.events[index], header=self.header
        )

    def __len__(self) -> int:
        return len(self.watch_response.events)


WatchCallback: TypeAlias = Callable[[Union[Exception, WatchResponse]], Any]


@dataclass
class _NewWatch:
    callback: WatchCallback
    id: Optional[int] = None
    err: Optional[Exception] = None


def _safe_callback(
    callback: WatchCallback, response_or_err: Union[Exception, WatchResponse]
) -> None:
    try:
        callback(response_or_err)
    except Exception:
        _log.exception("Watch callback failed")


# TODO: rewrite this


class Watcher:
    def __init__(
        self,
        watchstub: etcdrpc.WatchStub,
        timeout: Optional[int | float] = None,
        call_credentials: Optional[grpc.CallCredentials] = None,
        metadata: Optional[Tuple[Tuple[str, str], ...]] = None,
    ):
        self.timeout = timeout
        self._watch_stub = watchstub
        self._credentials = call_credentials
        self._metadata = metadata

        self._lock = threading.Lock()
        self._request_queue: queue.Queue[Union[etcdrpc.WatchRequest, None]] = (
            queue.Queue(maxsize=10)
        )

        self._callbacks: Dict[int, WatchCallback] = dict()
        self._callback_thread: Optional[threading.Thread] = None
        self._new_watch_cond = threading.Condition(lock=self._lock)
        self._new_watch: Optional[_NewWatch] = None

    def add_callback(
        self,
        key: Union[str, bytes],
        callback: WatchCallback,
        range_end: Optional[Union[str, bytes]] = None,
        start_revision: Optional[int] = None,
        progress_notify: bool = False,
        filters: Optional[Iterable[WatchFilterType]] = None,
        prev_kv: bool = False,
    ) -> int:
        rq = build_create_watch_request(
            key,
            range_end=range_end,
            start_revision=start_revision,
            progress_notify=progress_notify,
            filters=filters,
            prev_kv=prev_kv,
        )

        with self._lock:
            # Start the callback thread if it is not yet running.
            if not self._callback_thread:
                thread_name = "etcd3_watch_%x" % (id(self),)
                self._callback_thread = threading.Thread(
                    name=thread_name, target=self._run, daemon=True
                )

                self._callback_thread.start()

            # Only one create watch request can be pending at a time, so if
            # there one already, then wait for it to complete first.
            self._new_watch_cond.wait_for(lambda: self._new_watch is None)

            # Submit a create watch request.
            new_watch = _NewWatch(callback)
            self._request_queue.put(rq)
            self._new_watch = new_watch

            try:
                # Wait for the request to be completed, or timeout.
                self._new_watch_cond.wait(timeout=self.timeout)

                watch_id = new_watch.id

                if new_watch.err is not None:
                    raise new_watch.err

                if watch_id is None:
                    raise WatchTimedOut()

                return watch_id
            finally:
                # Wake up threads stuck on add_callback call if any.
                self._new_watch = None
                self._new_watch_cond.notify_all()

    def cancel(self, watch_id: int) -> None:
        with self._lock:
            callback = self._callbacks.pop(watch_id, None)
            if not callback:
                return

            self._cancel_no_lock(watch_id)

    def _run(self) -> None:
        def _new_request_iter(
            _request_queue: queue.Queue[Union[etcdrpc.WatchRequest, None]],
        ) -> Iterator[Union[etcdrpc.WatchRequest, None]]:
            while True:
                rq = _request_queue.get()
                if rq is None:
                    return

                yield rq

        while True:
            response_iter: Iterable[etcdrpc.WatchResponse] = self._watch_stub.Watch(
                _new_request_iter(self._request_queue),
                credentials=self._credentials,
                metadata=self._metadata,
            )

            try:
                for rs in response_iter:
                    self._handle_response(rs)

            except grpc.RpcError as err:
                with self._lock:
                    if self._new_watch:
                        self._new_watch.err = err
                        self._new_watch_cond.notify_all()

                    callbacks = self._callbacks
                    self._callbacks = {}

                    # Rotate request queue. This way we can terminate one gRPC
                    # stream and initiate another one whilst avoiding a race
                    # between them over requests in the queue.
                    self._request_queue.put(None)
                    self._request_queue = queue.Queue(maxsize=10)

                for callback in callbacks.values():
                    _safe_callback(callback, err)

    def _handle_response(self, rs: etcdrpc.WatchResponse) -> None:
        with self._lock:
            if rs.created:
                # If the new watch request has already expired then cancel the
                # created watch right away.
                if not self._new_watch:
                    self._cancel_no_lock(rs.watch_id)

                    return

                # XXX: don't think this can happen; at least on etcd 3.2 and 3.5, the watch is always created
                # and then right after that another response comes in with compact_revision; but just in case, keeping
                # this here
                if rs.compact_revision != 0:
                    self._new_watch.err = RevisionCompactedError(rs.compact_revision)
                    return

                self._callbacks[rs.watch_id] = self._new_watch.callback
                self._new_watch.id = rs.watch_id
                self._new_watch_cond.notify_all()

                callback: Optional[WatchCallback] = self._new_watch.callback
            elif rs.canceled or rs.compact_revision != 0:
                # watch got cancelled; either for some custom reason or due to compaction; get rid of the callback
                #
                # note: when watch is cancelled due to compaction, the behavior differs between etcd versions:
                # etcd3.2 - sends response with compact_revision field set
                # etcd3.5 - sends response with both canceled and compact_revision fields set
                callback = self._callbacks.pop(rs.watch_id, None)
            else:
                callback = self._callbacks.get(rs.watch_id)

        if not callback:
            return

        if rs.compact_revision != 0:
            # just send out the error; no need for explicit cancellation as the watch is already cancelled
            # by the server
            _safe_callback(callback, RevisionCompactedError(rs.compact_revision))

            return

        if rs.events or not (rs.created or rs.canceled):
            # call the callback even when there are no events in the watch
            # response so as not to ignore progress notify responses.
            response = WatchResponse(watch_response=rs)

            _safe_callback(callback, response)

    def _cancel_no_lock(self, watch_id: int) -> None:
        rq = etcdrpc.WatchRequest(
            cancel_request=etcdrpc.WatchCancelRequest(watch_id=watch_id)
        )

        self._request_queue.put(rq)
