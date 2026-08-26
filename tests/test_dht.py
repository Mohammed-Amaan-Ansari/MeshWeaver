from meshweaver.dht.node_id import (
    ID_BYTES,
    generate_node_id,
    node_id_to_int,
    node_id_to_hex,
    xor_distance,
)

from meshweaver.dht.routing_table import (
    PeerInfo,
    RoutingTable,
)


def test_generate_node_id():

    node_id = generate_node_id(
        "NODE_A"
    )

    assert isinstance(
        node_id,
        bytes,
    )

    assert len(node_id) == ID_BYTES


def test_deterministic_node_id():

    id_a = generate_node_id(
        "NODE_A"
    )

    id_b = generate_node_id(
        "NODE_A"
    )

    assert id_a == id_b


def test_node_id_conversion():

    node_id = generate_node_id(
        "NODE_A"
    )

    value = node_id_to_int(
        node_id
    )

    hex_value = node_id_to_hex(
        node_id
    )

    assert isinstance(
        value,
        int,
    )

    assert isinstance(
        hex_value,
        str,
    )

    assert len(hex_value) == 40


def test_xor_distance():

    id_a = generate_node_id(
        "NODE_A"
    )

    id_b = generate_node_id(
        "NODE_B"
    )

    distance = xor_distance(
        id_a,
        id_b,
    )

    assert distance >= 0


def test_routing_table():

    local_id = generate_node_id(
        "NODE_A"
    )

    peer_id = generate_node_id(
        "NODE_B"
    )

    table = RoutingTable(
        local_id
    )

    peer = PeerInfo(
        node_id=peer_id,
        host="127.0.0.1",
        port=9002,
    )

    added = table.add_peer(
        peer
    )

    assert added is True

    assert len(table) == 1


def test_routing_table_does_not_add_self():

    local_id = generate_node_id(
        "NODE_A"
    )

    table = RoutingTable(
        local_id
    )

    peer = PeerInfo(
        node_id=local_id,
        host="127.0.0.1",
        port=9001,
    )

    added = table.add_peer(
        peer
    )

    assert added is False

    assert len(table) == 0

def test_routing_table_bucket_selection():

    local_id = generate_node_id(
        "NODE_A"
    )

    peer_id = generate_node_id(
        "NODE_B"
    )

    table = RoutingTable(
        local_id
    )

    bucket = table.bucket_index(
        peer_id
    )

    assert bucket is not None

    assert 0 <= bucket < 160