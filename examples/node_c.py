import asyncio

from meshweaver.node import MeshNode


async def main():

    node = MeshNode(
        host="127.0.0.1",
        port=9003,
        node_id="NODE_C",

        # NODE_C connects to NODE_A
        bootstrap_peers=[
            ("127.0.0.1", 9001),
        ],
    )

    await node.start()


if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:

        print(
            "\n[NODE_C] "
            "Node stopped."
        )