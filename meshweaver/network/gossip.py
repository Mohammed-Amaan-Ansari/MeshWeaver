import asyncio

from meshweaver.network.discovery import (
    encode_message,
)

from meshweaver.utils.metrics import (
    get_system_load,
)


GOSSIP = "GOSSIP"


def create_gossip_message(node):

    load = get_system_load()

    return {
        "type": GOSSIP,
        "node_id": node.node_id,
        "load": load,
    }


async def gossip_loop(node):

    while True:

        await asyncio.sleep(5)

        # No peers available
        if not node.peers:
            continue

        message = create_gossip_message(
            node
        )

        data = encode_message(
            message
        )

        for peer in node.peers:

            node.transport.sendto(
                data,
                peer,
            )

        print(
            f"[{node.node_id}] "
            f"GOSSIP sent | "
            f"CPU: "
            f"{message['load']['cpu']:.1f}% | "
            f"RAM: "
            f"{message['load']['memory']:.1f}%"
        )