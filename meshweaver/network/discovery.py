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

async def discovery_loop(node, bootstrap_peers):
    while True:
        for peer in bootstrap_peers:
            if peer != (node.host, node.port):
                await send_hello(node, peer)

        await asyncio.sleep(10)