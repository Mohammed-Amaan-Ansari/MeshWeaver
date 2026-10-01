from meshweaver.security.config import SecurityConfig
from meshweaver.security.transport_security import (
    TransportSecurity,
    ReplayAttackError,
    InvalidMessageError,
    PacketTooLargeError,
    PeerAuthenticationError,
)


def section(title):
    print("\n" + "=" * 65)
    print(title)
    print("=" * 65)


def main():

    config = SecurityConfig.development()

    correct_key = config.key
    wrong_key = b"wrong-security-key-32-bytes-long!!"

    security = TransportSecurity(correct_key)
    attacker = TransportSecurity(wrong_key)

    payload = b"MeshWeaver Phase 5 security test"

    # =========================================================
    # TEST 1 — VALID PACKET
    # =========================================================

    section("TEST 1 - VALID HMAC PACKET")

    packet = security.protect(payload)

    try:
        recovered = security.unprotect(packet)

        if recovered == payload:
            print("PASS: Valid packet accepted.")
        else:
            print("FAIL: Payload was modified.")

    except Exception as exc:
        print(f"FAIL: Valid packet rejected: {exc}")

    # =========================================================
    # TEST 2 — WRONG KEY
    # =========================================================

    section("TEST 2 - WRONG HMAC KEY")

    packet = security.protect(payload)

    try:
        attacker.unprotect(packet)
        print("FAIL: Packet accepted with wrong key.")

    except InvalidMessageError:
        print("PASS: Wrong-key packet rejected.")

    except Exception as exc:
        print(f"PASS: Wrong-key packet rejected: {type(exc).__name__}")

    # =========================================================
    # TEST 3 — TAMPERED PACKET
    # =========================================================

    section("TEST 3 - TAMPERED PACKET")

    packet = security.protect(payload)

    tampered = bytearray(packet)

    # Change one byte in the payload area
    tampered[1 + security.NONCE_SIZE] ^= 0xFF

    try:
        security.unprotect(bytes(tampered))
        print("FAIL: Tampered packet accepted.")

    except InvalidMessageError:
        print("PASS: Tampered packet rejected.")

    except Exception as exc:
        print(f"PASS: Tampered packet rejected: {type(exc).__name__}")

    # =========================================================
    # TEST 4 — REPLAY ATTACK
    # =========================================================

    section("TEST 4 - REPLAY ATTACK")

    packet = security.protect(b"replay-test")

    # First use
    try:
        security.unprotect(packet)
        print("First delivery: PASS")
    except Exception as exc:
        print(f"FAIL: First delivery rejected: {exc}")

    # Replay the exact same packet
    try:
        security.unprotect(packet)
        print("FAIL: Replay packet accepted.")

    except ReplayAttackError:
        print("PASS: Replay packet rejected.")

    except Exception as exc:
        print(f"PASS: Replay packet rejected: {type(exc).__name__}")

    # =========================================================
    # TEST 5 — MALFORMED PACKET
    # =========================================================

    section("TEST 5 - MALFORMED PACKET")

    malformed_packet = b"bad"

    try:
        security.unprotect(malformed_packet)
        print("FAIL: Malformed packet accepted.")

    except Exception as exc:
        print(f"PASS: Malformed packet rejected: {type(exc).__name__}")

    # =========================================================
    # TEST 6 — PEER AUTHENTICATION
    # =========================================================

    section("TEST 6 - PEER AUTHENTICATION")

    challenge = security.create_peer_challenge()

    response = security.create_peer_response(
        challenge,
        "NODE_B",
    )

    try:
        result = security.verify_peer_response(
            challenge,
            "NODE_B",
            response,
        )

        if result:
            print("PASS: Valid peer authentication accepted.")

    except Exception as exc:
        print(f"FAIL: Valid peer authentication rejected: {exc}")

    # Wrong peer identity
    try:
        security.verify_peer_response(
            challenge,
            "ATTACKER",
            response,
        )

        print("FAIL: Authentication accepted for wrong peer.")

    except PeerAuthenticationError:
        print("PASS: Wrong peer identity rejected.")

    except Exception as exc:
        print(f"PASS: Wrong peer identity rejected: {type(exc).__name__}")

    # =========================================================
    # SUMMARY
    # =========================================================

    section("PHASE 5 SECURITY VERIFICATION")

    print("HMAC authentication       : TESTED")
    print("Message integrity         : TESTED")
    print("Wrong-key rejection       : TESTED")
    print("Tamper detection          : TESTED")
    print("Replay protection         : TESTED")
    print("Malformed packet handling : TESTED")
    print("Peer authentication       : TESTED")

    print("\nPhase 5 security tests finished.")


if __name__ == "__main__":
    main()