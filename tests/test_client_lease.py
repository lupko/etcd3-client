def test_lease_create(client1):
    lease = client1.lease(ttl=60)

    assert lease is not None
    assert lease.id > 0
    assert lease.ttl == 60


def test_lease_get_info(client1):
    lease = client1.lease(ttl=60)
    info = client1.get_lease_info(lease_id=lease.id)

    assert info.grantedTTL == 60
    assert info.TTL <= info.grantedTTL
    assert info.ID == lease.id


def test_lease_revoke(client1):
    lease = client1.lease(ttl=60)
    client1.revoke_lease(lease_id=lease.id)

    revoked_info = client1.get_lease_info(lease_id=lease.id)
    assert revoked_info.TTL == -1


def test_lease_keys(client1, keyspace):
    lease = client1.lease(ttl=60)
    client1.put(key=keyspace("test"), value=b"test", lease=lease)
    info = client1.get_lease_info(lease_id=lease.id)

    assert len(info.keys) == 1
    assert info.keys[0] == keyspace("test")


def test_lease_refresh(client1):
    lease = client1.lease(ttl=60)

    for refresh in client1.refresh_lease(lease_id=lease.id):
        assert refresh.TTL == 60


def test_lease_object_ttl_methods(client1):
    lease = client1.lease(ttl=60)

    assert lease.granted_ttl == 60
    assert lease.remaining_ttl <= lease.granted_ttl


def test_lease_object_revoke(client1):
    lease = client1.lease(ttl=60)
    lease.revoke()

    assert lease.remaining_ttl == -1
    assert lease.granted_ttl == 0


def test_lease_object_refresh(client1):
    lease = client1.lease(ttl=60)
    lease.refresh()

    assert lease.granted_ttl == 60


def test_lease_object_keys(client1, keyspace):
    lease = client1.lease(ttl=60)
    client1.put(key=keyspace("test"), value=b"test", lease=lease)

    keys = lease.keys
    assert len(keys) == 1
    assert keys[0] == keyspace("test")
