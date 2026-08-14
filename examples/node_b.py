import asyncio
from meshweaver.node import MeshNode


async def main():
    node = MeshNode(
        host="127.0.0.1",
        port=9002,
        bootstrap_peers=[
            ("127.0.0.1", 9001),
        ],
    )

    await node.start()


asyncio.run(main())