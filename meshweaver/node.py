import asyncio

from meshweaver.network.transport import start_udp_server

from meshweaver.scheduler.load_balancer import (
    select_best_peer,
)

from meshweaver.network.discovery import (
    HELLO,
    WELCOME,
    TASK,
    FIND_NODE,
    FIND_NODE_RESPONSE,
    create_hello,
    create_welcome,
    create_find_node,
    create_find_node_response,
    encode_message,
    decode_message,
)

from meshweaver.task.network import (
    extract_task,
)

from meshweaver.network.gossip import (
    GOSSIP,
    gossip_loop,
)

# =========================================================
# KAD MELIA DHT
# =========================================================

from meshweaver.dht.node_id import (
    generate_node_id,
    node_id_to_hex,
)

from meshweaver.dht.routing_table import (
    PeerInfo,
    RoutingTable,
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

        # =================================================
        # UDP TRANSPORT
        # =================================================

        self.transport = None

        # =================================================
        # EXISTING PEER NETWORK
        # =================================================

        # Known peers
        self.peers = set()

        # CPU/RAM information received from peers
        #
        # Example:
        #
        # {
        #     "NODE_B": {
        #         "cpu": 20.5,
        #         "memory": 40.2
        #     }
        # }

        self.peer_loads = {}

        # Initial peers used for discovery
        self.bootstrap_peers = (
            bootstrap_peers or []
        )

        # =================================================
        # KAD MELIA DHT
        # =================================================

        # Generate a deterministic 160-bit
        # DHT node ID from the MeshWeaver node ID.

        self.dht_node_id = generate_node_id(
            self.node_id
        )

        # Create Kademlia routing table.

        self.routing_table = RoutingTable(
            self.dht_node_id
        )

    # =====================================================
    # START NODE
    # =====================================================

    async def start(self):

        print("=" * 60)
        print("Starting MeshWeaver Node")
        print(f"Node ID : {self.node_id}")
        print(f"Address : {self.host}:{self.port}")

        print(
            f"DHT ID  : "
            f"{node_id_to_hex(self.dht_node_id)}"
        )

        print("=" * 60)

        # Start UDP server

        await start_udp_server(self)

        # Give UDP server time to initialize

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

    # =====================================================
    # PEER DISCOVERY
    # =====================================================

    async def discover_peers(self):

        if not self.bootstrap_peers:

            return

        message = create_hello(
            self.node_id,
            self.port,
        )

        data = encode_message(
            message
        )

        for peer in self.bootstrap_peers:

            # Don't send HELLO to ourselves

            if (
                peer[0] == self.host
                and peer[1] == self.port
            ):

                continue

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

            try:

                await self.discover_peers()

            except Exception as exc:

                print(
                    f"[{self.node_id}] "
                    f"Discovery error: {exc}"
                )

    # =====================================================
    # MESSAGE HANDLING
    # =====================================================

    async def handle_message(
        self,
        data,
        addr,
    ):

        try:

            message = decode_message(
                data
            )

        except Exception as exc:

            print(
                f"[{self.node_id}] "
                f"Invalid message: {exc}"
            )

            return

        message_type = message.get(
            "type"
        )

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

        elif message_type == TASK:

            await self.handle_task(
                message,
                addr,
            )
        elif message_type == FIND_NODE:
            await self.handle_find_node(
                message,
                addr,
            )

        elif message_type == FIND_NODE_RESPONSE:
            await self.handle_find_node_respons(
                message,
                addr,
            )
        

        else:

            print(
                f"[{self.node_id}] "
                f"Unknown message type: "
                f"{message_type}"
            )

    # =====================================================
    # HELLO
    # =====================================================

    async def handle_hello(
        self,
        message,
        addr,
    ):

        peer_id = message.get(
            "node_id"
        )

        # Ignore our own message

        if peer_id == self.node_id:

            return

        # Check whether this is a new peer

        is_new_peer = (
            addr not in self.peers
        )

        # Add to existing peer table

        self.peers.add(
            addr
        )

        # Add to Kademlia routing table

        self.add_peer_to_routing_table(
            peer_id,
            addr[0],
            addr[1],
        )

        if is_new_peer:

            print(
                f"\n[{self.node_id}] "
                f"Discovered peer: "
                f"{peer_id}"
            )

            self.print_peers()

            self.print_dht_table()

        # Send WELCOME response

        response = create_welcome(
            self.node_id,
            self.port,
        )

        self.transport.sendto(
            encode_message(response),
            addr,
        )

    # =====================================================
    # WELCOME
    # =====================================================

    async def handle_welcome(
        self,
        message,
        addr,
    ):

        peer_id = message.get(
            "node_id"
        )

        # Ignore ourselves

        if peer_id == self.node_id:

            return

        # Check whether this is a new peer

        is_new_peer = (
            addr not in self.peers
        )

        # Add to existing peer table

        self.peers.add(
            addr
        )

        # Add to Kademlia routing table

        self.add_peer_to_routing_table(
            peer_id,
            addr[0],
            addr[1],
        )

        if is_new_peer:

            print(
                f"\n[{self.node_id}] "
                f"Connected with peer: "
                f"{peer_id}"
            )

            self.print_peers()

            self.print_dht_table()

    # =====================================================
    # GOSSIP
    # =====================================================

    async def handle_gossip(
        self,
        message,
        addr,
    ):

        peer_id = message.get(
            "node_id"
        )

        load = message.get(
            "load"
        )

        # Ignore our own gossip

        if peer_id == self.node_id:

            return

        # Validate load

        if not isinstance(
            load,
            dict,
        ):

            print(
                f"[{self.node_id}] "
                f"Invalid load information "
                f"from {peer_id}"
            )

            return

        if (
            "cpu" not in load
            or "memory" not in load
        ):

            print(
                f"[{self.node_id}] "
                f"Incomplete load information "
                f"from {peer_id}"
            )

            return

        # Store peer load

        self.peer_loads[
            peer_id
        ] = load

        print(
            f"\n[{self.node_id}] "
            f"LOAD UPDATE from "
            f"{peer_id}"
        )

        print(
            f"   CPU : "
            f"{load['cpu']:.1f}%"
        )

        print(
            f"   RAM : "
            f"{load['memory']:.1f}%"
        )

        self.print_peer_loads()

    # =====================================================
    # TASK HANDLING
    # =====================================================

    async def handle_task(
        self,
        message,
        addr,
    ):

        print(
            f"\n[{self.node_id}] "
            f"TASK received from {addr}"
        )

        try:

            task = extract_task(
                message
            )

            print(
                f"[{self.node_id}] "
                f"Task extracted successfully"
            )

            return task

        except Exception as exc:

            print(
                f"[{self.node_id}] "
                f"Task extraction failed: "
                f"{exc}"
            )

            return None

    # =====================================================
    # LOAD BALANCER
    # =====================================================

    def get_best_peer(self):

        # No peer information available

        if not self.peer_loads:

            print(
                f"[{self.node_id}] "
                f"No peer load information "
                f"available."
            )

            return None

        # Select peer with lowest load

        best_peer = select_best_peer(
            self.peer_loads
        )

        print(
            f"[{self.node_id}] "
            f"Best peer: "
            f"{best_peer}"
        )

        return best_peer

    # =====================================================
    # PEER LOAD TABLE
    # =====================================================

    def print_peer_loads(self):

        print()

        print(
            f"[{self.node_id}] "
            f"PEER LOAD TABLE"
        )

        print("-" * 60)

        if not self.peer_loads:

            print(
                "No peer load information."
            )

            print("-" * 60)

            return

        for (
            peer_id,
            load,
        ) in self.peer_loads.items():

            cpu = load.get(
                "cpu",
                100,
            )

            memory = load.get(
                "memory",
                100,
            )

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

    # =====================================================
    # KAD MELIA DHT
    # =====================================================

    def add_peer_to_routing_table(
        self,
        peer_id,
        host,
        port,
    ):

        if not peer_id:

            return False

        # Don't add ourselves

        if peer_id == self.node_id:

            return False

        try:

            # Convert peer's MeshWeaver ID
            # into a deterministic 160-bit ID.

            peer_dht_id = (
                generate_node_id(
                    peer_id
                )
            )

            peer = PeerInfo(
                node_id=peer_dht_id,
                host=host,
                port=port,
            )

            added = (
                self.routing_table.add_peer(
                    peer
                )
            )

            if added:

                bucket_index = (
                    self.routing_table.bucket_index(
                        peer_dht_id
                    )
                )

                print(
                    f"[{self.node_id}] "
                    f"DHT peer added: "
                    f"{peer_id} "
                    f"(bucket "
                    f"{bucket_index})"
                )

            return added

        except Exception as exc:

            print(
                f"[{self.node_id}] "
                f"DHT peer error: "
                f"{exc}"
            )

            return False

    # =====================================================
    # DHT ROUTING TABLE
    # =====================================================

    def print_dht_table(self):

        print()

        print(
            f"[{self.node_id}] "
            f"KAD MELIA ROUTING TABLE"
        )

        print("-" * 70)

        total_peers = 0

        for (
            index,
            bucket,
        ) in enumerate(
            self.routing_table.buckets
        ):

            if not bucket:

                continue

            print(
                f"Bucket {index}:"
            )

            for peer in bucket:

                print(
                    f"   └── "
                    f"{peer.host}:"
                    f"{peer.port} "
                    f"ID="
                    f"{node_id_to_hex(peer.node_id)}"
                )

                total_peers += 1

        if total_peers == 0:

            print(
                "   No DHT peers."
            )

        print("-" * 70)

        print(
            f"Total DHT peers: "
            f"{total_peers}"
        )

    # =====================================================
    # FIND PEERS IN DHT BUCKET
    # =====================================================

    def get_dht_bucket_peers(
        self,
        peer_node_id,
    ):

        return (
            self.routing_table.find_bucket_peers(
                peer_node_id
            )
        )

    # =====================================================
    # ALL DHT PEERS
    # =====================================================

    def get_all_dht_peers(self):

        return (
            self.routing_table.get_all_peers()
        )

    # =====================================================
    # REMOVE PEER FROM DHT
    # =====================================================

    def remove_dht_peer(
        self,
        peer_node_id,
    ):

        removed = (
            self.routing_table.remove_peer(
                peer_node_id
            )
        )

        if removed:

            print(
                f"[{self.node_id}] "
                f"DHT peer removed."
            )

        return removed

    # =====================================================
    # EXISTING PEER TABLE
    # =====================================================

    def print_peers(self):

        print(
            f"[{self.node_id}] "
            f"Known peers: "
            f"{len(self.peers)}"
        )

        for (
            host,
            port,
        ) in self.peers:

            print(
                f"   └── "
                f"{host}:{port}"
            )

        # =====================================================
    # DHT PEER LOOKUP
    # =====================================================

    def find_closest_peers(
        self,
        target_node_id,
        count=3,
    ):
        """
        Find the closest peers to a target
        DHT node ID.
        """

        return self.routing_table.find_closest_peers(
            target_node_id,
            count,
        )

    # =====================================================
    # DHT LOOKUP BY NODE NAME
    # =====================================================

    def find_closest_peers_by_id(
        self,
        target_node_name,
        count=3,
    ):
        """
        Convert a MeshWeaver node name into a
        deterministic DHT ID and find the
        closest peers.
        """

        target_dht_id = generate_node_id(
            target_node_name
        )

        peers = self.find_closest_peers(
            target_dht_id,
            count,
        )

        print()
        print(
            f"[{self.node_id}] "
            f"DHT LOOKUP: "
            f"{target_node_name}"
        )

        print("-" * 60)

        if not peers:

            print(
                "No peers available."
            )

            print("-" * 60)

            return []

        for index, peer in enumerate(
            peers,
            start=1,
        ):

            distance = (
                self.routing_table.distance_to(
                    peer.node_id
                )
            )

            print(
                f"{index}. "
                f"{peer.host}:{peer.port} "
                f"| distance={distance}"
            )

        print("-" * 60)

        return peers