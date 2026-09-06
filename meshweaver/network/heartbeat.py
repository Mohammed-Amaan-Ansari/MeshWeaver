import asyncio
import time

from meshweaver.network.discovery import (
    HEARTBEAT,
    HEARTBEAT_ACK,
    create_heartbeat,
    encode_message,
)


# ============================================================
# CONFIGURATION
# ============================================================

HEARTBEAT_INTERVAL = 3
PEER_TIMEOUT = 8


# ============================================================
# HEARTBEAT LOOP
# ============================================================

async def heartbeat_loop(node):
    """
    Periodically send HEARTBEAT messages
    to all known peers.
    """

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

            try:

                node.transport.sendto(
                    data,
                    peer
                )

            except Exception as exc:

                print(
                    f"\n[{node.node_id}] "
                    f"Heartbeat send error "
                    f"to {peer}: {exc}"
                )


# ============================================================
# MARK PEER ALIVE
# ============================================================

def mark_peer_alive(
    node,
    peer_id,
    addr,
):
    """
    Mark a peer as alive.

    Updates:
        peer_last_seen
        peer_addresses

    Also removes the peer from dead_peers
    if it comes back online.
    """

    if peer_id == node.node_id:
        return

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


# ============================================================
# HANDLE HEARTBEAT
# ============================================================

async def handle_heartbeat(
    node,
    message,
    addr,
):
    """
    Handle an incoming HEARTBEAT.

    When another node sends us a heartbeat:
        1. Identify the peer.
        2. Mark it alive.
        3. Register its address.
        4. Send HEARTBEAT_ACK.
    """

    peer_id = message.get(
        "node_id"
    )

    if not peer_id:

        print(
            f"[{node.node_id}] "
            f"Invalid HEARTBEAT from {addr}"
        )

        return

    if peer_id == node.node_id:
        return

    # Mark peer as alive
    mark_peer_alive(
        node,
        peer_id,
        addr,
    )

    print(
        f"[{node.node_id}] "
        f"HEARTBEAT <- {peer_id} "
        f"at {addr}"
    )

    # --------------------------------------------------------
    # Send HEARTBEAT_ACK
    # --------------------------------------------------------

    if node.transport is None:
        return

    try:

        ack_message = {
            "type": HEARTBEAT_ACK,
            "node_id": node.node_id,
        }

        ack_data = encode_message(
            ack_message
        )

        node.transport.sendto(
            ack_data,
            addr
        )

    except Exception as exc:

        print(
            f"[{node.node_id}] "
            f"Heartbeat ACK error: {exc}"
        )


# ============================================================
# HANDLE HEARTBEAT ACK
# ============================================================

async def handle_heartbeat_ack(
    node,
    message,
    addr,
):
    """
    Handle HEARTBEAT_ACK from a peer.

    The ACK confirms that the peer is alive.
    """

    peer_id = message.get(
        "node_id"
    )

    if not peer_id:

        print(
            f"[{node.node_id}] "
            f"Invalid HEARTBEAT_ACK "
            f"from {addr}"
        )

        return

    if peer_id == node.node_id:
        return

    # Mark peer alive
    mark_peer_alive(
        node,
        peer_id,
        addr,
    )

    print(
        f"[{node.node_id}] "
        f"HEARTBEAT_ACK <- {peer_id}"
    )


# ============================================================
# FAILURE DETECTION
# ============================================================

async def failure_detection_loop(
    node
):
    """
    Detect peers that have stopped
    responding to heartbeat messages.
    """

    while True:

        await asyncio.sleep(
            2
        )

        now = time.time()

        for (
            peer_id,
            last_seen
        ) in list(
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

                    # Notify MeshNode about failure
                    try:

                        await node.handle_peer_failure(
                            peer_id
                        )

                    except Exception as exc:

                        print(
                            f"[{node.node_id}] "
                            f"Peer failure handling "
                            f"error: {exc}"
                        )