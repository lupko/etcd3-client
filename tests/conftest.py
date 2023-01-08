import os
import uuid
from typing import Callable, List, Tuple

import pytest
from typing_extensions import TypeAlias

import etcd3

__current_dir__ = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(scope="session")
def etcd_nodes(docker_ip, docker_services) -> List[Tuple[str, int]]:
    port = docker_services.port_for("etcd-test1", 2379)

    return [("localhost", port)]


@pytest.fixture(scope="session")
def client1(etcd_nodes):
    host, port = etcd_nodes[0]

    return etcd3.client(host=host, port=port)


KeyFactory: TypeAlias = Callable[[str], bytes]


@pytest.fixture(scope="function")
def keyspace() -> KeyFactory:
    random = uuid.uuid4().hex

    def _factory(key: str) -> bytes:
        return f"{random}/{key}".encode("utf-8")

    return _factory


@pytest.fixture(scope="session")
def certs() -> Tuple[str, str, str]:
    return (
        os.path.join(__current_dir__, "ca.crt"),
        os.path.join(__current_dir__, "client.crt"),
        os.path.join(__current_dir__, "client.key"),
    )
