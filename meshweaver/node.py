import asyncio
from meshweaver.network.gossip import (
    GOSSIP,
    gossip_loop,
)
from meshweaver.network.transport import start_udp_server
from meshweaver.network.discovery import (
    HELLO,
    WELCOME,
    create_hello,
    create_welcome,
    encode_message,
    decode_message,
)


class MeshNode:

    def __init__(self, host, port, node_id, bootstrap_peers=None):

        self.host = host
        self.port = port
        self.node_id = node_id

        self.transport = None

        # Stores discovered peers
        self.peers = set()
        
        self.peer_loads = {}

        # Initial peers supplied when starting the node
        self.bootstrap_peers = bootstrap_peers or []

    async def start(self):

        print()
        print("=" * 50)
        print(f"Starting MeshWeaver Node")
        print(f"Node ID : {self.node_id}")
        print(f"Address : {self.host}:{self.port}")
        print("=" * 50)

        await start_udp_server(self)

        # Give UDP server a moment to start
        await asyncio.sleep(1)

        # Introduce ourselves to bootstrap peers
        await self.discover_peers()

        # Periodically announce ourselves
        asyncio.create_task(self.discovery_loop())

        # Keep node alive
        await asyncio.Event().wait()

    async def discover_peers(self):

        message = create_hello(
            self.node_id,
            self.port
        )

        data = encode_message(message)

        for peer in self.bootstrap_peers:

            self.transport.sendto(
                data,
                peer
            )

            print(
                f"[{self.node_id[:8]}] "
                f"HELLO sent to {peer}"
            )

    async def discovery_loop(self):

        while True:

            await asyncio.sleep(10)

            await self.discover_peers()

    async def handle_message(self, data, addr):

        try:
            message = decode_message(data)

        except Exception as exc:

            print(
                f"[{self.node_id[:8]}] "
                f"Invalid message from {addr}: {exc}"
            )

            return

        message_type = message.get("type")

        if message_type == HELLO:

            await self.handle_hello(
                message,
                addr
            )

        elif message_type == WELCOME:

            await self.handle_welcome(
                message,
                addr
            )

    async def handle_hello(self, message, addr):

        peer_id = message.get("node_id")

        if peer_id == self.node_id:
            return

        new_peer = addr not in self.peers

        self.peers.add(addr)

        if new_peer:

            print(
                f"[{self.node_id[:8]}] "
                f"Discovered peer: {peer_id[:8]} "
                f"@ {addr}"
            )

            self.print_peers()

        # Send WELCOME response

        response = create_welcome(
            self.node_id,
            self.port
        )

        self.transport.sendto(
            encode_message(response),
            addr
        )

    async def handle_welcome(self, message, addr):

        peer_id = message.get("node_id")

        if peer_id == self.node_id:
            return

        new_peer = addr not in self.peers

        self.peers.add(addr)

        if new_peer:

            print(
                f"[{self.node_id[:8]}] "
                f"Connected with peer: {peer_id[:8]} "
                f"@ {addr}"
            )

            self.print_peers()

    def print_peers(self):

        print(
            f"\n[{self.node_id[:8]}] "
            f"Known peers: {len(self.peers)}"
        )

        for peer in self.peers:

            print(
                f"   └── {peer[0]}:{peer[1]}"
            )

        print()