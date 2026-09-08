from meshweaver.security.config import SecurityConfig
from meshweaver.security.transport_security import (
    TransportSecurity,
    ReplayAttackError,
)


def main():

    print("=" * 60)
    print("MeshWeaver Replay Attack Protection Test")
    print("=" * 60)

    # ---------------------------------------------------------
    # Create shared security key
    # ---------------------------------------------------------

    config = SecurityConfig.development()

    sender_security = TransportSecurity(
        config.key
    )

    receiver_security = TransportSecurity(
        config.key
    )

    # ---------------------------------------------------------
    # Create legitimate packet
    # ---------------------------------------------------------

    payload = b"HELLO NODE B"

    print("\n[1] Creating secure packet...")

    packet = sender_security.protect(
        payload
    )

    print(
        f"    Payload: {payload!r}"
    )

    print(
        f"    Packet size: {len(packet)} bytes"
    )

    # ---------------------------------------------------------
    # First transmission
    # ---------------------------------------------------------

    print("\n[2] First transmission...")

    received = receiver_security.unprotect(
        packet
    )

    print(
        "SUCCESS: First packet accepted."
    )

    print(
        f"    Received: {received!r}"
    )

    # ---------------------------------------------------------
    # Replay same packet
    # ---------------------------------------------------------

    print("\n[3] Replaying the exact same packet...")

    try:

        receiver_security.unprotect(
            packet
        )

        print(
            "ERROR: Replay attack was accepted!"
        )

    except ReplayAttackError as exc:

        print(
            "SUCCESS: Replay attack rejected."
        )

        print(
            f"    Reason: {exc}"
        )

    # ---------------------------------------------------------
    # New packet with same payload
    # ---------------------------------------------------------

    print(
        "\n[4] Sending a new packet "
        "with the same payload..."
    )

    new_packet = sender_security.protect(
        payload
    )

    try:

        new_received = (
            receiver_security.unprotect(
                new_packet
            )
        )

        print(
            "SUCCESS: New packet accepted."
        )

        print(
            f"    Received: {new_received!r}"
        )

    except Exception as exc:

        print(
            "ERROR: New packet was rejected."
        )

        print(
            f"    Reason: {exc}"
        )

    print("\n" + "=" * 60)
    print(
        "Replay Attack Protection Test Complete"
    )
    print("=" * 60)


if __name__ == "__main__":
    main()