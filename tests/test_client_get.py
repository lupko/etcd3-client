from typing import Tuple

import pytest
from conftest import KeyFactory

import etcd3
import etcd3.base


@pytest.fixture(scope="session")
def get_fixture(client1) -> Tuple[etcd3.Etcd3Client, KeyFactory]:
    """
    Fixture for all tests in this module. Since this is running read-only tests, the fixture can be
    module-level.
    """

    def _keyspace(key: str) -> bytes:
        return f"get_tests/{key}".encode("utf-8")

    # each group intentionally put separately to have a 'segment' of create revisions
    # per prefix

    # note that the keys are intentionally not added in alphabetical order; the prefix goes
    # the opposite direction

    # also note the create revision (and mod revision) is assigned in-order of PUTs

    for i in range(10):
        client1.put(_keyspace(f"test2/{i}"), f"test1/val/{i}".encode("utf-8"))

    for i in range(10):
        client1.put(_keyspace(f"test1/{i}"), f"test1/val/{i}".encode("utf-8"))

    for i in range(10):
        client1.put(_keyspace(f"test/{i}"), f"test/val/{i}".encode("utf-8"))

    return client1, _keyspace


def test_get(get_fixture):
    client1, keyspace = get_fixture

    test_key = keyspace("test/0")
    val, meta = client1.get(test_key)

    assert val is not None
    assert isinstance(val, bytes)
    assert val == b"test/val/0"

    assert meta is not None
    assert isinstance(meta, etcd3.base.KVMetadata)
    assert meta.key == test_key
    assert meta.version == 1
    assert meta.create_revision > 0
    assert meta.mod_revision > 0


def test_get_missing(get_fixture):
    client1, keyspace = get_fixture

    val, meta = client1.get(keyspace("does/not/exist"))

    assert val is None
    assert meta is None


def test_get_strict(get_fixture):
    client1, keyspace = get_fixture

    test_key = keyspace("test/1")
    val, meta = client1.get_strict(test_key)

    assert val == b"test/val/1"
    assert meta.key == test_key
    assert meta.version == 1
    assert meta.create_revision > 0
    assert meta.mod_revision > 0


def test_get_strict_missing(get_fixture):
    client1, keyspace = get_fixture
    test_key = keyspace("does/not/exist")

    with pytest.raises(KeyError):
        client1.get_strict(test_key)


def test_get_prefix(get_fixture):
    client1, keyspace = get_fixture
    prefix = keyspace("test/")

    results = list(client1.get_prefix(prefix))

    assert len(results) == 10
    for value, meta in results:
        assert meta.key.startswith(prefix)


def test_get_prefix_keys_only(get_fixture):
    client1, keyspace = get_fixture
    prefix = keyspace("test/")

    results = list(client1.get_prefix(prefix, keys_only=True))

    assert len(results) == 10
    for value, meta in results:
        assert meta.key.startswith(prefix)
        assert value == b""


def test_get_prefix_sort_order(get_fixture):
    client1, keyspace = get_fixture
    prefix = keyspace("test/")
    first_key = keyspace("test/0")
    last_key = keyspace("test/9")

    results = list(client1.get_prefix(prefix))

    assert len(results) == 10
    assert results[0][1].key == first_key
    assert results[-1][1].key == last_key

    results = list(client1.get_prefix(prefix, sort_order="desc"))

    assert len(results) == 10
    assert results[0][1].key == last_key
    assert results[-1][1].key == first_key

    results = list(client1.get_prefix(prefix, sort_order="asc"))

    assert len(results) == 10
    assert results[0][1].key == first_key
    assert results[-1][1].key == last_key


def test_get_prefix_sort_target(get_fixture):
    client1, keyspace = get_fixture
    prefix = keyspace("test/")
    first_key = keyspace("test/0")
    last_key = keyspace("test/9")

    results = list(client1.get_prefix(prefix, sort_target="create"))

    assert len(results) == 10
    assert results[0][1].key == first_key
    assert results[-1][1].key == last_key

    results = list(client1.get_prefix(prefix, sort_order="desc", sort_target="create"))

    assert len(results) == 10
    assert results[0][1].key == last_key
    assert results[-1][1].key == first_key

    results = list(client1.get_prefix(prefix, sort_order="asc", sort_target="create"))

    assert len(results) == 10
    assert results[0][1].key == first_key
    assert results[-1][1].key == last_key


def test_get_prefix_paged(get_fixture):
    client1, keyspace = get_fixture
    prefix = keyspace("test/")

    results = list(client1.get_prefix(prefix))
    paged_results = list(client1.get_prefix_paged(prefix, page_size=3))

    assert len(paged_results) == len(results)
    for paged_result, non_paged_result in zip(paged_results, results):
        assert paged_result[1].key == non_paged_result[1].key
        assert paged_result[0] == non_paged_result[0]


def test_get_prefix_paged_keys_only(get_fixture):
    client1, keyspace = get_fixture
    prefix = keyspace("test/")

    results = list(client1.get_prefix(prefix, keys_only=True))
    paged_results = list(client1.get_prefix_paged(prefix, keys_only=True, page_size=3))

    assert len(paged_results) == len(results)
    for paged_result, non_paged_result in zip(paged_results, results):
        assert paged_result[1].key == non_paged_result[1].key
        assert paged_result[0] == non_paged_result[0]


def test_get_range1(get_fixture):
    client1, keyspace = get_fixture
    range_start = keyspace("test/")
    range_end = keyspace("test1")

    results = list(client1.get_range(range_start, range_end))

    assert len(results) == 10
    for _, meta in results:
        assert meta.key.startswith(range_start)


def test_get_range2(get_fixture):
    client1, keyspace = get_fixture
    range_start = keyspace("test/")
    middle = keyspace("test1/")
    range_end = keyspace("test2")

    results = list(client1.get_range(range_start, range_end))

    assert len(results) == 20
    for _, meta in results:
        assert meta.key.startswith(range_start) or meta.key.startswith(middle)


def test_get_range_empty(get_fixture):
    client1, keyspace = get_fixture
    range_start = keyspace("does/not/exist/begin")
    range_end = keyspace("does/not/exist/begin")

    results = list(client1.get_range(range_start, range_end))

    assert len(results) == 0


def test_get_range_keys_only(get_fixture):
    client1, keyspace = get_fixture
    range_start = keyspace("test/")
    middle = keyspace("test1/")
    range_end = keyspace("test2")

    results = list(client1.get_range(range_start, range_end, keys_only=True))

    assert len(results) == 20
    for value, meta in results:
        assert value == b""
        assert meta.key.startswith(range_start) or meta.key.startswith(middle)


def test_get_range_sort_order(get_fixture):
    client1, keyspace = get_fixture
    range_start = keyspace("test/")
    range_end = keyspace("test2")

    first_key = keyspace("test/0")
    last_key = keyspace("test1/9")

    results = list(client1.get_range(range_start, range_end))

    assert results[0][1].key == first_key
    assert results[-1][1].key == last_key

    results = list(client1.get_range(range_start, range_end, sort_order="asc"))

    assert results[0][1].key == first_key
    assert results[-1][1].key == last_key

    results = list(client1.get_range(range_start, range_end, sort_order="desc"))

    assert results[0][1].key == last_key
    assert results[-1][1].key == first_key


def test_get_range_sort_target(get_fixture):
    client1, keyspace = get_fixture
    range_start = keyspace("test/")
    range_end = keyspace("test2")

    first_key = keyspace("test1/0")
    last_key = keyspace("test/9")

    results = list(client1.get_range(range_start, range_end, sort_target="create"))

    assert results[0][1].key == first_key
    assert results[-1][1].key == last_key

    results = list(
        client1.get_range(
            range_start, range_end, sort_order="asc", sort_target="create"
        )
    )

    assert results[0][1].key == first_key
    assert results[-1][1].key == last_key

    results = list(
        client1.get_range(
            range_start, range_end, sort_order="desc", sort_target="create"
        )
    )

    assert results[0][1].key == last_key
    assert results[-1][1].key == first_key


#
# get all tests have to do client side filtering before assertion... that is because they can run
# after all the put/delete tests. while those tests work in their isolated sub-spaces.. the get-all is impacted
# by them because it well.. gets all the keys
#


def test_get_all(get_fixture):
    client1, keyspace = get_fixture

    results = list([res for res in client1.get_all() if res[1].key.startswith(b"get_")])

    # get all gets everything; since etcd is shared other testcases may have created
    # keys; the get_fixture contains 30 keys so at least those must be there
    assert len(results) == 30


def test_get_all_keys_only(get_fixture):
    client1, keyspace = get_fixture

    results = list(
        [
            res
            for res in client1.get_all(keys_only=True)
            if res[1].key.startswith(b"get_")
        ]
    )

    assert len(results) >= 30
    for value, meta in results:
        assert value == b""


def test_get_all_keys_only_with_limit(get_fixture):
    client1, keyspace = get_fixture

    results = list(client1.get_all(keys_only=True, limit=5))

    assert len(results) == 5


def test_get_range_paged1(get_fixture):
    client1, keyspace = get_fixture
    range_start = keyspace("test")
    range_end = keyspace("xxx")

    results = list(client1.get_range(range_start=range_start, range_end=range_end))
    paged_results = list(
        client1.get_range_paged(
            range_start=range_start, range_end=range_end, page_size=1
        )
    )

    assert len(paged_results) == len(results)

    for paged_result, non_paged_result in zip(paged_results, results):
        assert paged_result[1].key == non_paged_result[1].key
        assert paged_result[0] == non_paged_result[0]


def test_get_range_paged2(get_fixture):
    client1, keyspace = get_fixture
    range_start = keyspace("test")
    range_end = keyspace("xxx")

    results = list(client1.get_range(range_start=range_start, range_end=range_end))
    paged_results = list(
        client1.get_range_paged(
            range_start=range_start, range_end=range_end, page_size=2
        )
    )

    assert len(paged_results) == len(results)

    for paged_result, non_paged_result in zip(paged_results, results):
        assert paged_result[1].key == non_paged_result[1].key
        assert paged_result[0] == non_paged_result[0]


def test_get_range_paged3(get_fixture):
    client1, keyspace = get_fixture
    range_start = keyspace("test")
    range_end = keyspace("xxx")

    results = list(client1.get_range(range_start=range_start, range_end=range_end))
    paged_results = list(
        client1.get_range_paged(
            range_start=range_start, range_end=range_end, page_size=3
        )
    )

    assert len(paged_results) == len(results)
    for paged_result, non_paged_result in zip(paged_results, results):
        assert paged_result[1].key == non_paged_result[1].key
        assert paged_result[0] == non_paged_result[0]


def test_get_range_paged_keys_only(get_fixture):
    client1, keyspace = get_fixture
    range_start = keyspace("test")
    range_end = keyspace("xxx")

    results = list(
        client1.get_range(range_start=range_start, range_end=range_end, keys_only=True)
    )
    paged_results = list(
        client1.get_range_paged(
            range_start=range_start, range_end=range_end, page_size=3, keys_only=True
        )
    )

    assert len(paged_results) == len(results)

    for paged_result, non_paged_result in zip(paged_results, results):
        assert paged_result[1].key == non_paged_result[1].key
        assert paged_result[0] == non_paged_result[0]


def test_get_all_sort_order(get_fixture):
    client1, keyspace = get_fixture
    first_key = keyspace("test/0")
    last_key = keyspace("test2/9")

    results = list([res for res in client1.get_all() if res[1].key.startswith(b"get_")])

    assert results[0][1].key == first_key
    assert results[-1][1].key == last_key

    results = list(
        [
            res
            for res in client1.get_all(sort_order="asc")
            if res[1].key.startswith(b"get_")
        ]
    )

    assert results[0][1].key == first_key
    assert results[-1][1].key == last_key

    results = list(
        [
            res
            for res in client1.get_all(sort_order="desc")
            if res[1].key.startswith(b"get_")
        ]
    )

    assert results[0][1].key == last_key
    assert results[-1][1].key == first_key


def test_get_all_sort_target(get_fixture):
    client1, keyspace = get_fixture
    first_key = keyspace("test2/0")
    last_key = keyspace("test/9")

    results = list(
        [
            res
            for res in client1.get_all(sort_target="create")
            if res[1].key.startswith(b"get_")
        ]
    )

    assert results[0][1].key == first_key
    assert results[-1][1].key == last_key

    results = list(
        [
            res
            for res in client1.get_all(sort_target="create", sort_order="asc")
            if res[1].key.startswith(b"get_")
        ]
    )

    assert results[0][1].key == first_key
    assert results[-1][1].key == last_key

    results = list(
        [
            res
            for res in client1.get_all(sort_target="create", sort_order="desc")
            if res[1].key.startswith(b"get_")
        ]
    )

    assert results[0][1].key == last_key
    assert results[-1][1].key == first_key


# note: intentionally keeping this test as it adds extra key and this would break
# the various get range tests
def test_get_revision(get_fixture):
    client1, keyspace = get_fixture

    _, meta = client1.get(keyspace("test/0"))
    revision_without_new_key = meta.create_revision

    # add some new test key
    test_key = keyspace("test_get_revision")
    response = client1.put(test_key, "value1")
    # keep rev at which it was created with the original value
    original_rev = response.header.revision
    # and also create new revision with the new value
    client1.put(test_key, "value2")

    # getting latest revision -> most recent state
    val, _ = client1.get(test_key)
    assert val == b"value2"

    # getting revision at which the key was created -> original state after first put
    val, _ = client1.get(test_key, revision=original_rev)
    assert val == b"value1"

    # getting revision at which the key did not exist -> no result
    val, meta = client1.get(test_key, revision=revision_without_new_key)
    assert val is None

    # getting prefix (range) at latest revision - gets the key
    prefix_get = list(client1.get_prefix(keyspace("test_")))
    assert len(prefix_get) == 1

    # getting prefix (range) at revision when the key did not exist - no result
    prefix_get = list(
        client1.get_prefix(keyspace("test_"), revision=revision_without_new_key)
    )
    assert len(prefix_get) == 0
