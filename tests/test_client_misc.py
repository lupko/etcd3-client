import pytest

import etcd3
import etcd3.rpc as etcdrpc


def test_status(client1):
    status = client1.status()

    assert len(status.version)
    assert status.leader is not None
    assert status.leader.id > 0
    assert status.raft_term > 0
    assert status.raft_index > 0


def test_members(client1):
    members = list(client1.members)

    assert len(members) == 3

    for m in members:
        assert m.id > 0
        assert len(m.client_urls)
        assert len(m.peer_urls)


def test_defragment(client1):
    response = client1.defragment()

    assert response is not None


def test_alarm(client1):
    alarms = client1.create_alarm(alarm_type="corrupt")

    assert len(alarms) == 1
    assert alarms[0].member_id == 0
    assert alarms[0].alarm_type == etcdrpc.AlarmType.CORRUPT

    alarms = list(client1.list_alarms(alarm_type="corrupt"))
    assert len(alarms) == 1
    assert alarms[0].member_id == 0
    assert alarms[0].alarm_type == etcdrpc.AlarmType.CORRUPT

    client1.disarm_alarm(alarm_type="corrupt")
    alarms = list(client1.list_alarms(alarm_type="corrupt"))

    assert len(alarms) == 0


def test_use_secure_channel(etcd_nodes, certs):
    host, port = etcd_nodes[0]
    cacert, client, key = certs

    client = etcd3.client(host, port, ca_cert=cacert, cert_cert=client, cert_key=key)
    assert client.uses_secure_channel

    client = etcd3.client(host, port, ca_cert=cacert)
    assert client.uses_secure_channel


def test_invalid_secure_channel_conf(etcd_nodes, certs):
    host, port = etcd_nodes[0]
    cacert, client, key = certs

    with pytest.raises(ValueError):
        etcd3.client(host=host, port=port, ca_cert=cacert, cert_cert=None, cert_key=key)

    with pytest.raises(ValueError):
        etcd3.client(
            host=host, port=port, ca_cert=cacert, cert_cert=client, cert_key=None
        )
