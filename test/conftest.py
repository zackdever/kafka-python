import os

import pytest

from test.fixtures import KafkaFixture, ZookeeperFixture, FixtureManager


@pytest.fixture(scope="module")
def version():
    if 'KAFKA_VERSION' not in os.environ:
        return ()
    return tuple(map(int, os.environ['KAFKA_VERSION'].split('.')))


@pytest.fixture(scope="module")
def zookeeper(version, request):
    assert version
    zk = ZookeeperFixture.instance()
    def fin():
        zk.close()
    request.addfinalizer(fin)
    return zk


@pytest.fixture(scope="module")
def kafka_broker(version, zookeeper, request):
    assert version
    k = KafkaFixture.instance(0, zookeeper.host, zookeeper.port,
                              partitions=4)
    # TODO maybe pass delay option to instance that defaults to False
    FixtureManager.open_instances(k)
    def fin():
        k.close()
    request.addfinalizer(fin)
    return k
