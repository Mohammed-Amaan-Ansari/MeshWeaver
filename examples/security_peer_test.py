from meshweaver.security.config import SecurityConfig
from meshweaver.security.transport_security import (
    TransportSecurity,
    PeerAuthenticationError,
)


def main():

    print("=" * 60)
    print("MeshWeaver Peer Authentication Test")
    print("=" * 60)

    config = SecurityConfig.development()

    node_a_security = TransportSecurity(
        config.key
    )

    node_b_security = TransportSecurity(
        config.key
    )

    # ---------------------------------------------------------
    # 1. Create challenge
    # ---------------------------------------------------------

    challenge = (
        node_a_security.create_peer_challenge()
    )

    print(
        "\n[1] Challenge generated"
    )

    # ---------------------------------------------------------
    # 2. Node B creates response
    # ---------------------------------------------------------

    response = (
        node_b_security.create_peer_response(
            challenge,
            "NODE_B",
        )
    )

    print(
        "[2] NODE_B generated authentication response"
    )

    # ---------------------------------------------------------
    # 3. Node A verifies response
    # ---------------------------------------------------------

    authenticated = (
        node_a_security.verify_peer_response(
            challenge,
            "NODE_B",
            response,
        )
    )

    print(
        "[3] NODE_A verified NODE_B"
    )

    print(
        f"    Authentication result: {authenticated}"
    )

    # ---------------------------------------------------------
    # 4. Test invalid response
    # ---------------------------------------------------------

    print(
        "\n[4] Testing invalid authentication..."
    )

    bad_response = bytearray(response)

    bad_response[0] ^= 0xFF

    try:

        node_a_security.verify_peer_response(
            challenge,
            "NODE_B",
            bytes(bad_response),
        )

        print(
            "ERROR: Invalid response was accepted!"
        )

    except PeerAuthenticationError as exc:

        print(
            "SUCCESS: Invalid response rejected."
        )

        print(
            f"    Reason: {exc}"
        )

    print("\n" + "=" * 60)
    print("Peer Authentication Test Complete")
    print("=" * 60)


if __name__ == "__main__":
    main()