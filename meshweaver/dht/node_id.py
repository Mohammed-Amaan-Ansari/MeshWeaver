import hashlib
import secrets


ID_BITS = 160
ID_BYTES = ID_BITS // 8


def generate_node_id(node_name=None):
    """
    Generate a 160-bit identifier for a MeshWeaver node.

    If node_name is provided, the ID is generated
    deterministically from that name.

    Otherwise, a random 160-bit ID is generated.
    """

    if node_name is not None:

        digest = hashlib.sha1(
            node_name.encode("utf-8")
        ).digest()

        return digest

    return secrets.token_bytes(ID_BYTES)


def node_id_to_int(node_id):
    """
    Convert a 160-bit node ID into an integer.
    """

    if not isinstance(node_id, bytes):

        raise TypeError(
            "node_id must be bytes"
        )

    if len(node_id) != ID_BYTES:

        raise ValueError(
            f"node_id must be "
            f"{ID_BYTES} bytes"
        )

    return int.from_bytes(
        node_id,
        byteorder="big",
    )


def node_id_to_hex(node_id):
    """
    Convert node ID into readable hexadecimal form.
    """

    if not isinstance(node_id, bytes):

        raise TypeError(
            "node_id must be bytes"
        )

    return node_id.hex()


def xor_distance(node_id_a, node_id_b):
    """
    Calculate XOR distance between two node IDs.

    Kademlia uses XOR distance to determine
    how close two nodes are in the DHT.
    """

    if len(node_id_a) != ID_BYTES:

        raise ValueError(
            "Invalid first node ID"
        )

    if len(node_id_b) != ID_BYTES:

        raise ValueError(
            "Invalid second node ID"
        )

    return int.from_bytes(
        node_id_a,
        byteorder="big",
    ) ^ int.from_bytes(
        node_id_b,
        byteorder="big",
    )