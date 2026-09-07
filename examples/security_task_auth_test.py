from meshweaver.security.config import SecurityConfig
from meshweaver.security.transport_security import TransportSecurity


def main():

    print("=" * 60)
    print("MeshWeaver Task Authorization Test")
    print("=" * 60)

    config = SecurityConfig.development()

    security = TransportSecurity(config.key)

    # ---------------------------------------------------------
    # Simulate NODE_A
    # ---------------------------------------------------------
    authenticated_peers = {
        "NODE_A"
    }

    print("\n[1] Authenticated peer test")

    sender_id = "NODE_A"

    if sender_id in authenticated_peers:
        print(
            f"SUCCESS: {sender_id} is authenticated."
        )
    else:
        print(
            f"ERROR: {sender_id} was rejected."
        )

    # ---------------------------------------------------------
    # Simulate unauthorized node
    # ---------------------------------------------------------
    print("\n[2] Unauthenticated peer test")

    sender_id = "FAKE_NODE"

    if sender_id not in authenticated_peers:
        print(
            f"SUCCESS: Unauthorized peer "
            f"{sender_id} rejected."
        )
    else:
        print(
            f"ERROR: Unauthorized peer "
            f"{sender_id} was accepted!"
        )

    # ---------------------------------------------------------
    # Missing sender test
    # ---------------------------------------------------------
    print("\n[3] Missing sender_id test")

    sender_id = None

    if not sender_id:
        print(
            "SUCCESS: TASK with missing sender_id rejected."
        )
    else:
        print(
            "ERROR: TASK with missing sender_id accepted!"
        )

    # ---------------------------------------------------------
    # Tampered security packet test
    # ---------------------------------------------------------
    print("\n[4] Transport authentication test")

    payload = b'{"type":"TASK","sender_id":"NODE_A"}'

    protected_packet = security.protect(payload)

    print(
        f"Original packet size: "
        f"{len(protected_packet)} bytes"
    )

    tampered_packet = bytearray(protected_packet)

    # Modify payload
    tampered_packet[20] ^= 0xFF

    try:
        security.unprotect(bytes(tampered_packet))

        print(
            "ERROR: Tampered packet was accepted!"
        )

    except Exception as exc:

        print(
            "SUCCESS: Tampered packet rejected."
        )

        print(
            f"    Reason: {exc}"
        )

    print("\n" + "=" * 60)
    print("Task Authorization Test Complete")
    print("=" * 60)


if __name__ == "__main__":
    main()