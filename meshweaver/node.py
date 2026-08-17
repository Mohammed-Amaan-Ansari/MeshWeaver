import asyncio

from meshweaver.network.transport import start_udp_server

from meshweaver.scheduler.load_balancer import (
    select_best_peer,
)

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

        # UDP transport
        self.transport = None

        # Known peers
        self.peers = set()

        # CPU/RAM information received from peers
        #
        # Example:
        # {
        #     "NODE_B": {
        #         "cpu": 20.5,
        #         "memory": 40.2
        #     }
        # }
        self.peer_loads = {}

        # Initial peers used for discovery
        self.bootstrap_peers = bootstrap_peers or []

    async def start(self):

        print("=" * 50)
        print("Starting MeshWeaver Node")
        print(f"Node ID : {self.node_id}")
        print(f"Address : {self.host}:{self.port}")
        print("=" * 50)

        # Start UDP server
        await start_udp_server(self)

        # Give the UDP server time to start
        await asyncio.sleep(1)

        # Initial peer discovery
        await self.discover_peers()

        # Periodic peer discovery
        asyncio.create_task(
            self.discovery_loop()
        )

        # Periodic CPU/RAM gossip
        asyncio.create_task(
            gossip_loop(self)
        )

        # Keep node alive
        await asyncio.Event().wait()

    # ---------------------------------------------------------
    # PEER DISCOVERY
    # ---------------------------------------------------------

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

    # ---------------------------------------------------------
    # MESSAGE HANDLING
    # ---------------------------------------------------------

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

        else:

            print(
                f"[{self.node_id}] "
                f"Unknown message type: {message_type}"
            )

    # ---------------------------------------------------------
    # HELLO
    # ---------------------------------------------------------

    async def handle_hello(
        self,
        message,
        addr,
    ):

        peer_id = message.get("node_id")

        # Ignore our own message
        if peer_id == self.node_id:
            return

        # Check whether this is a new peer
        is_new_peer = addr not in self.peers

        # Add peer
        self.peers.add(addr)

        if is_new_peer:

            print(
                f"\n[{self.node_id}] "
                f"Discovered peer: {peer_id}"
            )

            self.print_peers()

        # Send WELCOME response
        response = create_welcome(
            self.node_id,
            self.port,
        )

        self.transport.sendto(
            encode_message(response),
            addr,
        )

    # ---------------------------------------------------------
    # WELCOME
    # ---------------------------------------------------------

    async def handle_welcome(
        self,
        message,
        addr,
    ):

        peer_id = message.get("node_id")

        # Ignore ourselves
        if peer_id == self.node_id:
            return

        # Check whether this is a new peer
        is_new_peer = addr not in self.peers

        # Add peer
        self.peers.add(addr)

        if is_new_peer:

            print(
                f"\n[{self.node_id}] "
                f"Connected with peer: {peer_id}"
            )

            self.print_peers()

    # ---------------------------------------------------------
    # GOSSIP
    # ---------------------------------------------------------

    async def handle_gossip(
        self,
        message,
        addr,
    ):

        peer_id = message.get("node_id")
        load = message.get("load")

        # Ignore our own gossip
        if peer_id == self.node_id:
            return

        # Validate load information
        if not isinstance(load, dict):
            print(
                f"[{self.node_id}] "
                f"Invalid load information from {peer_id}"
            )
            return

        if "cpu" not in load or "memory" not in load:
            print(
                f"[{self.node_id}] "
                f"Incomplete load information from {peer_id}"
            )
            return

        # Store peer load
        self.peer_loads[peer_id] = load

        print(
            f"\n[{self.node_id}] "
            f"LOAD UPDATE from {peer_id}"
        )

        print(
            f"   CPU : {load['cpu']:.1f}%"
        )

        print(
            f"   RAM : {load['memory']:.1f}%"
        )

        # Display current load table
        self.print_peer_loads()

    # ---------------------------------------------------------
    # LOAD BALANCER
    # ---------------------------------------------------------

    def get_best_peer(self):

        # No peer information available
        if not self.peer_loads:

            print(
                f"[{self.node_id}] "
                "No peer load information available."
            )

            return None

        # Select peer with lowest load
        best_peer = select_best_peer(
            self.peer_loads
        )

        print(
            f"[{self.node_id}] "
            f"Best peer: {best_peer}"
        )

        return best_peer

    def print_peer_loads(self):

        print()
        print(
            f"[{self.node_id}] PEER LOAD TABLE"
        )

        print("-" * 60)

        if not self.peer_loads:

            print("No peer load information.")

            print("-" * 60)

            return

        for peer_id, load in self.peer_loads.items():

            cpu = load.get("cpu", 100)
            memory = load.get("memory", 100)

            score = (
                cpu + memory
            ) / 2

            print(
                f"{peer_id:10} "
                f"CPU: {cpu:5.1f}% | "
                f"RAM: {memory:5.1f}% | "
                f"SCORE: {score:5.1f}"
            )

        print("-" * 60)

    # ---------------------------------------------------------
    # PEER TABLE
    # ---------------------------------------------------------

    def print_peers(self):

        print(
            f"[{self.node_id}] "
            f"Known peers: {len(self.peers)}"
        )

        for host, port in self.peers:

            print(
                f"   └── {host}:{port}"
            )