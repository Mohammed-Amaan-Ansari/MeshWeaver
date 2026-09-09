from meshweaver.security.config import SecurityConfig
from meshweaver.security.transport_security import (
    TransportSecurity,
    InvalidMessageError,
)


def main():

    print("=" * 60)
    print("MeshWeaver Transport Tamper Detection Test")
    print("=" * 60)

    # ---------------------------------------------------------
    # Create sender and receiver with the same security key
    # ---------------------------------------------------------

    config = SecurityConfig.development()

    sender_security = TransportSecurity(
        config.key
    )

    receiver_security = TransportSecurity(
        config.key
    )

    # ---------------------------------------------------------
    # Create a legitimate packet
    # ---------------------------------------------------------

    payload = b"HELLO NODE B"

    print("\n[1] Creating secure packet...")

    packet = sender_security.protect(
        payload
    )

    print(
        f"    Original payload: {payload!r}"
    )

    print(
        f"    Secure packet size: {len(packet)} bytes"
    )

    # ---------------------------------------------------------
    # Verify original packet
    # ---------------------------------------------------------

    print("\n[2] Testing original packet...")

    try:

        received = receiver_security.unprotect(
            packet
        )

        print(
            "SUCCESS: Original packet accepted."
        )

        print(
            f"    Received payload: {received!r}"
        )

    except Exception as exc:

        print(
            "ERROR: Original packet was rejected."
        )

        print(
            f"    Reason: {exc}"
        )

    # ---------------------------------------------------------
    # Tamper with packet
    # ---------------------------------------------------------

    print("\n[3] Tampering with packet...")

    tampered_packet = bytearray(packet)

    # Change one byte in the packet.
    #
    # The packet structure is:
    #
    # VERSION | NONCE | PAYLOAD | HMAC
    #
    # Index 0  = VERSION
    # Index 1-16 = NONCE
    # Index 17+ = PAYLOAD
    #
    # Modify the first payload byte.
    tampered_packet[17] ^= 0xFF

    tampered_packet = bytes(
        tampered_packet
    )

    print(
        "    Packet modified."
    )

    # ---------------------------------------------------------
    # Send tampered packet
    # ---------------------------------------------------------

    print(
        "\n[4] Sending tampered packet..."
    )

    try:

        receiver_security.unprotect(
            tampered_packet
        )

        print(
            "ERROR: Tampered packet was accepted!"
        )

    except InvalidMessageError as exc:

        print(
            "SUCCESS: Tampered packet rejected."
        )

        print(
            f"    Reason: {exc}"
        )

    except Exception as exc:

        print(
            "SUCCESS: Tampered packet rejected."
        )

        print(
            f"    Reason: {exc}"
        )

    # ---------------------------------------------------------
    # Summary
    # ---------------------------------------------------------

    print("\n" + "=" * 60)
    print("Tamper Detection Test Complete")
    print("=" * 60)


if __name__ == "__main__":
    main()