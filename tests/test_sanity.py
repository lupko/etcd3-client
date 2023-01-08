import etcd3

#
# verify that the minimum required core functionality works:
#
# 1. client can connect
# 2. client can put a key
# 3. client can get a key back
#
# Assert that these three pieces operate correctly; they are relied on in the remainder of the client
# tests.
#
# If this test fails, then a lot of other tests are bound to fail too. Was considering using something
# like pytest-dependency to codify this; it's nice, but it means extra typing for every single test
#


def test_sanity(client1: etcd3.Etcd3Client, keyspace):
    assert client1 is not None

    test_key = keyspace("smoke")

    put_response = client1.put(test_key, b"test")
    assert put_response is not None

    value, meta = client1.get_strict(test_key)
    assert value == b"test"
    assert meta.key == test_key
    assert meta.create_revision == put_response.header.revision
    assert meta.mod_revision == put_response.header.revision
    assert meta.version == 1
