from meshweaver.security.config import SecurityConfig
from meshweaver.security.transport_security import (
    TransportSecurity,
    InvalidMessageError,
    ReplayAttackError,
    PacketTooLargeError,
)


def main():

    print("=" * 60)
    print("MeshWeaver Security Hardening Test")
    print("=" * 60)

    config = SecurityConfig.development()

    sender = TransportSecurity(
        config.key
    )

    receiver = TransportSecurity(
        config.key
    )

    # =========================================================
    # Test 1: Valid packet
    # =========================================================

    print("\n[1] Valid packet test")

    payload = b"HELLO NODE B"

    packet = sender.protect(
        payload
    )

    try:

        result = receiver.unprotect(
            packet
        )

        print(
            "SUCCESS: Valid packet accepted."
        )

        print(
            f"    Payload: {result!r}"
        )

    except Exception as exc:

        print(
            f"ERROR: Valid packet rejected: {exc}"
        )

    # =========================================================
    # Test 2: Replay attack
    # =========================================================

    print("\n[2] Replay attack test")

    try:

        receiver.unprotect(
            packet
        )

        print(
            "ERROR: Replay packet accepted!"
        )

    except ReplayAttackError:

        print(
            "SUCCESS: Replay packet rejected."
        )

    # =========================================================
    # Test 3: Tampering
    # =========================================================

    print("\n[3] Tampered packet test")

    fresh_packet = sender.protect(
        b"IMPORTANT TASK"
    )

    tampered = bytearray(
        fresh_packet
    )

    # Change one byte in the payload.
    tampered[20] ^= 0xFF

    try:

        receiver.unprotect(
            bytes(tampered)
        )

        print(
            "ERROR: Tampered packet accepted!"
        )

    except InvalidMessageError:

        print(
            "SUCCESS: Tampered packet rejected."
        )

    # =========================================================
    # Test 4: Too-large payload
    # =========================================================

    print("\n[4] Oversized payload test")

    oversized_payload = (
        b"A"
        * (
            TransportSecurity.MAX_PAYLOAD_SIZE
            + 1
        )
    )

    try:

        sender.protect(
            oversized_payload
        )

        print(
            "ERROR: Oversized payload accepted!"
        )

    except PacketTooLargeError:

        print(
            "SUCCESS: Oversized payload rejected."
        )

    # =========================================================
    # Test 5: Invalid packet
    # =========================================================

    print("\n[5] Malformed packet test")

    malformed_packet = b"\x01\x02\x03"

    try:

        receiver.unprotect(
            malformed_packet
        )

        print(
            "ERROR: Malformed packet accepted!"
        )

    except InvalidMessageError:

        print(
            "SUCCESS: Malformed packet rejected."
        )

    print("\n" + "=" * 60)
    print(
        "Security Hardening Test Complete"
    )
    print("=" * 60)


if __name__ == "__main__":
    main()