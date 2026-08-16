import asyncio

from meshweaver.network.transport import start_udp_server

from meshweaver.network.discovery import (
    HELLO,
    WELCOME,
    create_hello,
    create_welcome,
    encode_message,
    decode_message,
)

from meshweaver.network.gossip import (
    GOSSIP,
    gossip_loop,
)


class MeshNode:

    def __init__(
        self,
        host,
        port,
        node_id,
        bootstrap_peers=None,
    ):

        self.host = host
        self.port = port
        self.node_id = node_id

        self.transport = None

        # Known peers
        self.peers = set()

        # CPU/RAM information of peers
        self.peer_loads = {}

        # Initial peers
        self.bootstrap_peers = bootstrap_peers or []

    async def start(self):

        print("=" * 50)
        print("Starting MeshWeaver Node")
        print(f"Node ID : {self.node_id}")
        print(f"Address : {self.host}:{self.port}")
        print("=" * 50)

        await start_udp_server(self)

        await asyncio.sleep(1)

        # Initial peer discovery
        await self.discover_peers()

        # Periodic discovery
        asyncio.create_task(
            self.discovery_loop()
        )

        # Periodic CPU/RAM gossip
        asyncio.create_task(
            gossip_loop(self)
        )

        # Keep node alive
        await asyncio.Event().wait()

    async def discover_peers(self):

        message = create_hello(
            self.node_id,
            self.port,
        )

        data = encode_message(message)

        for peer in self.bootstrap_peers:

            self.transport.sendto(
                data,
                peer,
            )

            print(
                f"[{self.node_id}] "
                f"HELLO → {peer}"
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
                f"[{self.node_id}] "
                f"Invalid message: {exc}"
            )

            return

        message_type = message.get("type")

        if message_type == HELLO:

            await self.handle_hello(
                message,
                addr,
            )

        elif message_type == WELCOME:

            await self.handle_welcome(
                message,
                addr,
            )

        elif message_type == GOSSIP:

            await self.handle_gossip(
                message,
                addr,
            )

    async def handle_hello(self, message, addr):

        peer_id = message.get("node_id")

        if peer_id == self.node_id:
            return

        is_new_peer = addr not in self.peers

        self.peers.add(addr)

        if is_new_peer:

            print(
                f"\n[{self.node_id}] "
                f"Discovered peer: {peer_id}"
            )

            self.print_peers()

        response = create_welcome(
            self.node_id,
            self.port,
        )

        self.transport.sendto(
            encode_message(response),
            addr,
        )

    async def handle_welcome(self, message, addr):

        peer_id = message.get("node_id")

        if peer_id == self.node_id:
            return

        is_new_peer = addr not in self.peers

        self.peers.add(addr)

        if is_new_peer:

            print(
                f"\n[{self.node_id}] "
                f"Connected with peer: {peer_id}"
            )

            self.print_peers()

    async def handle_gossip(self, message, addr):

        peer_id = message.get("node_id")
        load = message.get("load")

        if peer_id == self.node_id:
            return

        self.peer_loads[peer_id] = load

        print(
            f"\n[{self.node_id}] "
            f"LOAD UPDATE from {peer_id}"
        )

        print(
            f"   CPU : {load['cpu']}%"
        )

        print(
            f"   RAM : {load['memory']}%"
        )

    def print_peers(self):

        print(
            f"[{self.node_id}] "
            f"Known peers: {len(self.peers)}"
        )

        for host, port in self.peers:

            print(
                f"   └── {host}:{port}"
            )