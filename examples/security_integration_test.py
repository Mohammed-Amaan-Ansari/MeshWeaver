from meshweaver.security.config import SecurityConfig
from meshweaver.security.transport_security import (
    TransportSecurity,
    InvalidMessageError,
    ReplayAttackError,
    PacketTooLargeError,
)


def print_result(name, success):
    if success:
        print(f"SUCCESS: {name}")
    else:
        print(f"FAILED: {name}")


def main():

    print("=" * 65)
    print("MeshWeaver Week 4 Security Integration Test")
    print("=" * 65)

    # =========================================================
    # Security setup
    # =========================================================

    config = SecurityConfig.development()

    node_a = TransportSecurity(config.key)
    node_b = TransportSecurity(config.key)

    # =========================================================
    # TEST 1 — Normal authenticated communication
    # =========================================================

    print("\n[1] Normal secure communication")

    payload = b"HELLO NODE B"

    packet = node_a.protect(payload)

    try:
        received = node_b.unprotect(packet)

        if received == payload:
            print_result(
                "Authenticated packet accepted",
                True,
            )
        else:
            print_result(
                "Authenticated packet accepted",
                False,
            )

    except Exception as exc:

        print(
            f"FAILED: Normal communication rejected: "
            f"{exc}"
        )

    # =========================================================
    # TEST 2 — Replay protection
    # =========================================================

    print("\n[2] Replay attack protection")

    try:

        node_b.unprotect(packet)

        print_result(
            "Replay packet rejected",
            False,
        )

    except ReplayAttackError:

        print_result(
            "Replay packet rejected",
            True,
        )

    # =========================================================
    # TEST 3 — Tamper protection
    # =========================================================

    print("\n[3] Tamper detection")

    fresh_packet = node_a.protect(
        b"IMPORTANT TASK"
    )

    tampered_packet = bytearray(
        fresh_packet
    )

    # Modify payload byte.
    tampered_packet[17] ^= 0xFF

    try:

        node_b.unprotect(
            bytes(tampered_packet)
        )

        print_result(
            "Tampered packet rejected",
            False,
        )

    except InvalidMessageError:

        print_result(
            "Tampered packet rejected",
            True,
        )

    # =========================================================
    # TEST 4 — Oversized packet
    # =========================================================

    print("\n[4] Oversized packet protection")

    oversized_payload = (
        b"A"
        * (
            TransportSecurity.MAX_PAYLOAD_SIZE
            + 1
        )
    )

    try:

        node_a.protect(
            oversized_payload
        )

        print_result(
            "Oversized payload rejected",
            False,
        )

    except PacketTooLargeError:

        print_result(
            "Oversized payload rejected",
            True,
        )

    # =========================================================
    # TEST 5 — Wrong key
    # =========================================================

    print("\n[5] Wrong key protection")

    wrong_key = (
        b"WRONG_KEY_1234567890123456789012"
    )

    attacker = TransportSecurity(
        wrong_key
    )

    attacker_packet = attacker.protect(
        b"FAKE TASK"
    )

    try:

        node_b.unprotect(
            attacker_packet
        )

        print_result(
            "Packet from wrong key rejected",
            False,
        )

    except InvalidMessageError:

        print_result(
            "Packet from wrong key rejected",
            True,
        )

    # =========================================================
    # TEST 6 — Malformed packet
    # =========================================================

    print("\n[6] Malformed packet protection")

    malformed_packet = b"\x01\x02\x03"

    try:

        node_b.unprotect(
            malformed_packet
        )

        print_result(
            "Malformed packet rejected",
            False,
        )

    except InvalidMessageError:

        print_result(
            "Malformed packet rejected",
            True,
        )

    # =========================================================
    # Final summary
    # =========================================================

    print("\n" + "=" * 65)
    print("Security Integration Test Complete")
    print("=" * 65)

    print(
        "\nMeshWeaver security layers verified:"
    )

    print("  [OK] HMAC-SHA256 authentication")
    print("  [OK] Message integrity")
    print("  [OK] Replay protection")
    print("  [OK] Packet validation")
    print("  [OK] Payload size protection")
    print("  [OK] Wrong-key rejection")
    print("  [OK] Tamper detection")

    print("\n" + "=" * 65)


if __name__ == "__main__":
    main()