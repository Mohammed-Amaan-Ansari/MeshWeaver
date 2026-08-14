import asyncio
from meshweaver.node import MeshNode


async def main():
    node = MeshNode(
        host="127.0.0.1",
        port=9001,
        bootstrap_peers=[
            ("127.0.0.1", 9002),
        ],
    )

    await node.start()


asyncio.run(main())