import asyncio
import time

from meshweaver.network.discovery import (
    HEARTBEAT,
    create_heartbeat,
    encode_message,
)


HEARTBEAT_INTERVAL = 3

PEER_TIMEOUT = 8


async def heartbeat_loop(node):

    while True:

        await asyncio.sleep(
            HEARTBEAT_INTERVAL
        )

        if node.transport is None:
            continue

        message = create_heartbeat(
            node.node_id
        )

        data = encode_message(
            message
        )

        for peer in list(node.peers):

            node.transport.sendto(
                data,
                peer
            )


def mark_peer_alive(
    node,
    peer_id,
    addr,
):

    node.peer_last_seen[
        peer_id
    ] = time.time()

    node.peer_addresses[
        peer_id
    ] = addr

    if peer_id in node.dead_peers:

        node.dead_peers.remove(
            peer_id
        )

        print(
            f"\n[{node.node_id}] "
            f"PEER BACK ONLINE: "
            f"{peer_id}"
        )


async def failure_detection_loop(
    node
):

    while True:

        await asyncio.sleep(2)

        now = time.time()

        for peer_id, last_seen in list(
            node.peer_last_seen.items()
        ):

            elapsed = (
                now - last_seen
            )

            if elapsed > PEER_TIMEOUT:

                if (
                    peer_id
                    not in node.dead_peers
                ):

                    node.dead_peers.add(
                        peer_id
                    )

                    print(
                        f"\n[{node.node_id}] "
                        f"PEER OFFLINE: "
                        f"{peer_id}"
                    )

                    print(
                        f"   Last seen: "
                        f"{elapsed:.1f}s ago"
                    )