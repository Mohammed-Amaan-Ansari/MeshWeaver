import asyncio

from meshweaver.node import MeshNode
from meshweaver.security.config import SecurityConfig


async def main():
    config = SecurityConfig.development()

    node = MeshNode(
        host="127.0.0.1",
        port=9004,
        node_id="DHT_TESTER",
        bootstrap_peers=[
            ("127.0.0.1", 9001),
            ("127.0.0.1", 9002),
            ("127.0.0.1", 9003),
        ],
        security_key=config.key,
    )

    print("=" * 65)
    print("MeshWeaver - Phase 4 DHT Verification")
    print("=" * 65)

    await node.start_components()

    print("\nWaiting for peer discovery...")
    await asyncio.sleep(5)

    # --------------------------------------------------
    # 1. ROUTING TABLE
    # --------------------------------------------------

    print("\n" + "=" * 65)
    print("1. DHT ROUTING TABLE")
    print("=" * 65)

    node.print_dht_table()

    # --------------------------------------------------
    # 2. FIND_NODE
    # --------------------------------------------------

    print("\n" + "=" * 65)
    print("2. FIND_NODE")
    print("=" * 65)

    await node.find_node(
        ("127.0.0.1", 9001),
        "NODE_B",
    )

    await asyncio.sleep(2)

    # --------------------------------------------------
    # 3. STORE
    # --------------------------------------------------

    print("\n" + "=" * 65)
    print("3. STORE")
    print("=" * 65)

    test_key = "phase4_test_key"
    test_value = "MeshWeaver_DHT_SUCCESS"

    await node.store_value(
        ("127.0.0.1", 9002),
        test_key,
        test_value,
    )

    await asyncio.sleep(2)

    # --------------------------------------------------
    # 4. FIND_VALUE
    # --------------------------------------------------

    print("\n" + "=" * 65)
    print("4. FIND_VALUE")
    print("=" * 65)

    await node.find_value(
        ("127.0.0.1", 9002),
        test_key,
    )

    await asyncio.sleep(3)

    print("\n" + "=" * 65)
    print("PHASE 4 DHT VERIFICATION FINISHED")
    print("=" * 65)

    await node.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nDHT demo stopped.")