import hashlib


ID_BITS = 160
ID_BYTES = 20


def generate_node_id(
    node_name: str,
) -> bytes:

    if not isinstance(
        node_name,
        str,
    ):
        raise TypeError(
            "node_name must be a string"
        )

    return hashlib.sha1(
        node_name.encode("utf-8")
    ).digest()


def node_id_to_hex(
    node_id: bytes,
) -> str:

    if not isinstance(
        node_id,
        bytes,
    ):
        raise TypeError(
            "node_id must be bytes"
        )

    if len(node_id) != ID_BYTES:
        raise ValueError(
            "node_id must contain "
            "20 bytes"
        )

    return node_id.hex()


def hex_to_node_id(
    value: str,
) -> bytes:

    if not isinstance(
        value,
        str,
    ):
        raise TypeError(
            "value must be a string"
        )

    result = bytes.fromhex(value)

    if len(result) != ID_BYTES:
        raise ValueError(
            "DHT ID must contain 20 bytes"
        )

    return result


def xor_distance(
    node_id_a: bytes,
    node_id_b: bytes,
) -> int:

    if not isinstance(
        node_id_a,
        bytes,
    ):
        raise TypeError(
            "First node ID must be bytes"
        )

    if not isinstance(
        node_id_b,
        bytes,
    ):
        raise TypeError(
            "Second node ID must be bytes"
        )

    if len(node_id_a) != ID_BYTES:
        raise ValueError(
            "Invalid first node ID"
        )

    if len(node_id_b) != ID_BYTES:
        raise ValueError(
            "Invalid second node ID"
        )

    return (
        int.from_bytes(
            node_id_a,
            byteorder="big",
        )
        ^
        int.from_bytes(
            node_id_b,
            byteorder="big",
        )
    )