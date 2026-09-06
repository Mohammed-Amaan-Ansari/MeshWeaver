from meshweaver.dht.node_id import (
    generate_node_id,
    xor_distance,
)

from meshweaver.dht.routing_table import (
    PeerInfo,
    RoutingTable,
)

from meshweaver.dht.storage import (
    DHTStorage,
)


def test_node_id_is_deterministic():

    first = generate_node_id(
        "NODE_A"
    )

    second = generate_node_id(
        "NODE_A"
    )

    assert first == second

    assert len(first) == 20


def test_xor_distance():

    a = generate_node_id(
        "NODE_A"
    )

    b = generate_node_id(
        "NODE_B"
    )

    assert xor_distance(
        a,
        b,
    ) > 0


def test_routing_table():

    local = generate_node_id(
        "NODE_A"
    )

    remote = generate_node_id(
        "NODE_B"
    )

    table = RoutingTable(
        local
    )

    peer = PeerInfo(
        node_id=remote,
        host="127.0.0.1",
        port=9002,
    )

    assert table.add_peer(
        peer
    )

    assert len(table) == 1


def test_dht_storage():

    storage = DHTStorage()

    storage.store(
        "name",
        "MeshWeaver",
    )

    assert storage.exists(
        "name"
    )

    assert storage.get(
        "name"
    ) == "MeshWeaver"

    assert len(storage) == 1