import asyncio
from meshweaver.node import MeshNode


async def main():
    node = MeshNode("127.0.0.1", 9002)
    await node.start()


asyncio.run(main())