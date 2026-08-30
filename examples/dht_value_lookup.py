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

     