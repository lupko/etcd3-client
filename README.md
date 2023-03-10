# etcd3 client for Python

This is a pure Python package implementing client for [etcd](https://etcd.io/).

### Recognition

This package is forked from the now-dead [python-etcd3](https://github.com/kragniz/python-etcd3/) - it is based on
the last released version of the python-etcd3 which is 0.12.0; the [AUTHORS](./AUTHORS.md) lists all committers
to the original package.

## Changelog

### 0.91.0

- API Improvements - running transactions
  - Added new API to run transactions. The `txn()` method can be used to run transactions specified using `TransactionBuilder`
  - The `TransactionBuilder` provides fluent APIs through which code can incrementally build the transaction - specify
    compares, success and failure operations as necessary
  - The `TransactionResponse` is returned by the `txn()`. This is a facade that allows for a type-safe way to access
    responses for particular operations


### Initial version

- BREAKING changes
  - Dropped support for Python 2
  - Supports Python 3.9, 3.10 and 3.11 only; older versions of Python 3 are not supported
  - Works only with the last version of protobuf v3 (3.20.0); works with protobuf v4
  - Removed the locks module
  - Client methods no longer use **kwargs. Use of kwargs was replaced by having all supported parameters coded
    explicitly. This should not be a breaking change for typical usage. May be room for breakage in some more obscure
    usage patterns.
  - All custom objects returned by the client are now read-only. The read-only API is compatible with 0.12.0, however
    updates are not possible where they previously were possible
  - the Member object no longer contains manipulation methods (remove, update); it is a pure read-only data class.
    Methods on the client class still exist.
  - KVMetadata, Event, PutEvent and DeleteEvent classes use slots; your code may be broken if you added custom
    attributes to them
  - The client.watch_once() now returns all events from the first-encountered WatchResponse - as a List of events.
    As opposed to returning only single Event - the first one found in the WatchResponse
  - put_if_not_exists() now returns a `Tuple[bool, Optional[etcdrpc.PutResponse]]`. The bool is success indicator. If
    the operation succeeds, a tuple of True and the PutResponse is returned. Else False and None response are returned.
  - replace() now returns a `Tuple[bool, Optional[etcdrpc.PutResponse]`. The bool is success indicator. If the
    operation succeeds, a tuple of True and the PutResponse is returned. Else False and None response are returned.
  - delete() now returns a `Tuple[bool, Optional[etcdrpc.DeleteRangeReponse]`. The bool is success indicator. If
    the operation succeeds, a tuple of True and DeleteRangeResponse is returned. Else False and None response are
    returned.

- API Improvements
  - Events now include `kv_meta` and `prev_kv_meta` with `KVMetadata` objects
  - All code that forms public API is now re-exported
  - All client methods are explicitly typed and there is no use of opaque kwargs anywhere

- FIXES
  - Default call to `transaction` was failing due to success and failure being None
  - Sending `filter` to `watch` methods did not work
  - Not all gRPC timeouts were converted to etcd3.ConnectionTimedOut. gRPC error with status UNKNOWN and
    'context deadline exceeded' slipped through

- Modernization on top of 0.12.0
    - Changed how protobufs are generated; now uses buf.build
    - Generated protobufs can be consumed by latest version of protobuf (v4)
    - Adopted mypy
    - Reworked tests; now uses fully dockerized test environment (via pytest-docker)

## Contributing

After you clone the repository, make sure to initialize the development environment using `make dev`. This will set
up virtual environment for the project with all the dependencies installed. If you use direnv tool, do `direnv allow`
to auto-activate the virtual environment - otherwise activate manually using `source .envrc`.

With virtual environment activated, you can:

- Run tests using `make test` or using `pytest`.
- Run static type checks using `make mypy`
- Force run all pre-commit checks using `make fix-all` (they are otherwise done automatically during pre-commit hook)

If you want to start an etcd3 cluster for some ad-hoc dev testing, you can navigate to the `tests` directory and do
`docker-compose up -d`. Just keep in mind that you have to bring this cluster down (`docker-compose down -v`) before
you run the automated tests.

## License

Apache License 2.0. See [LICENSE](./LICENSE).
