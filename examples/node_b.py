import asyncio
from meshweaver.node import MeshNode


async def main():
    node = MeshNode("127.0.0.1", 9002)

    # Send a ping to Node A
    asyncio.create_task(
        node.send_ping("127.0.0.1", 9001)
    )

    await node.start()


asyncio.run(main())