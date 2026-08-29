import asyncio

from meshweaver.node import MeshNode


async def main():

    node = MeshNode(
        host="127.0.0.1",
        port=9005,
        node_id="STORAGE_TEST",
        bootstrap_peers=[
            ("127.0.0.1", 9001),
        ],
    )

    # Start node in background.

    asyncio.create_task(
        node.start()
    )

    # Allow networking to start.

    await asyncio.sleep(3)

    # Store value on NODE_A.

    await node.store_value(
        ("127.0.0.1", 9001),
        "task:1001",
        "process_image",
    )

    # Wait for response.

    await asyncio.sleep(2)

    # Ask NODE_A for the value.

    await node.find_value(
        ("127.0.0.1", 9001),
        "task:1001",
    )

    # Wait for response.

    await asyncio.sleep(5)


if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        print(
            "\n[DHT_STORAGE] "
            "Stopped."
        )