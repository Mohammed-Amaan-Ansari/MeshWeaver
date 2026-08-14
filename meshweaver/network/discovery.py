import asyncio
import json


async def send_hello(node, peer):
    message = {
        "type": "HELLO",
        "node_id": node.node_id[:8],
        "port": node.port,
    }

    node.transport.sendto(
        json.dumps(message).encode(),
        peer,
    )

    print(f"👋 HELLO sent to {peer}")