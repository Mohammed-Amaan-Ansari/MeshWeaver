import asyncio

from meshweaver.node import MeshNode


async def main():

    node = MeshNode(
        host="127.0.0.1",
        port=9001,
        node_id="NODE_A",
        bootstrap_peers=[
            ("127.0.0.1", 9002),
        ],
    )

    await node.start()


if __name__ == "__main__":

    asyncio.run(main())