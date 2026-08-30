import asyncio

from meshweaver.node import MeshNode


async def main():

    node = MeshNode(
        host="127.0.0.1",
        port=9005,
        node_id="VALUE_LOOKUP_NODE",
        bootstrap_peers=[
            ("127.0.0.1", 9001),
        ],
    )

    # Start node in background
    asyncio.create_task(
        node.start()
    )

    # Give the node time to start
    await asyncio.sleep(3)

    # Ask NODE_A for a value
    await node.find_value(
        ("127.0.0.1", 9001),
        "meshweaver:test",
    )

    # Keep node alive
    await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("\nVALUE LOOKUP NODE stopped.")