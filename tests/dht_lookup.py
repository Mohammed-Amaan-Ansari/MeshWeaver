import asyncio

from meshweaver.node import MeshNode


async def main():

    node = MeshNode(
        host="127.0.0.1",
        port=9004,
        node_id="LOOKUP_NODE",
        bootstrap_peers=[
            ("127.0.0.1", 9001),
        ],
    )

    # Start the node in the background.

    asyncio.create_task(
        node.start()
    )

    # Give networking and discovery
    # time to initialize.

    await asyncio.sleep(3)

    # Ask NODE_A to find peers close
    # to NODE_C's DHT ID.

    await node.find_node(
        ("127.0.0.1", 9001),
        "NODE_C",
    )

    # Keep the program alive so the
    # response can arrive.

    await asyncio.sleep(5)


if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        print(
            "\n[DHT_LOOKUP] "
            "Stopped."
        )