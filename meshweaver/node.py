import asyncio

from meshweaver.network.transport import start_udp_server

from meshweaver.dht.storage import DHTStorage

from meshweaver.scheduler.load_balancer import (
    select_best_peer,
)

from meshweaver.network.discovery import (
    HELLO,
    WELCOME,
    TASK,
    FIND_NODE,
    FIND_NODE_RESPONSE,
    STORE,
    STORE_RESPONSE,
    FIND_VALUE,
    FIND_VALUE_RESPONSE,
    create_hello,
    create_welcome,
    create_find_node,
    create_find_node_response,
    create_store,
    create_store_response,
    create_find_value,
    create_find_value_response,
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
        # PEERS
        # =================================================

        self.peers = set()

        # =================================================
        # PEER LOAD INFORMATION
        # =================================================

        self.peer_loads = {}

        # =================================================
        # BOOTSTRAP PEERS
        # =================================================

        self.bootstrap_peers = (
            bootstrap_peers or []
        )

        # =================================================
        # KADEMLIA DHT
        # =================================================

        self.dht_node_id = generate_node_id(
            self.node_id
        )

        self.routing_table = RoutingTable(
            self.dht_node_id
        )

        # =================================================
        # DHT STORAGE
        # =================================================

        self.dht_storage = DHTStorage()

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

        await asyncio.sleep(1)

        # Initial peer discovery
        await self.discover_peers()

        # Periodic discovery
        asyncio.create_task(
            self.discovery_loop()
        )

        # Periodic gossip
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

        data = encode_message(message)

        for peer in self.bootstrap_peers:

            # Don't contact ourselves
            if (
                peer[0] == self.host
                and peer[1] == self.port
            ):
                continue

            try:

                self.transport.sendto(
                    data,
                    peer,
                )

                print(
                    f"[{self.node_id}] "
                    f"HELLO → {peer}"
                )

            except Exception as exc:

                print(
                    f"[{self.node_id}] "
                    f"HELLO failed: {exc}"
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

            message = decode_message(data)

        except Exception as exc:

            print(
                f"[{self.node_id}] "
                f"Invalid message: {exc}"
            )

            return

        message_type = message.get(
            "type"
        )

        # -------------------------------------------------
        # HELLO
        # -------------------------------------------------

        if message_type == HELLO:

            await self.handle_hello(
                message,
                addr,
            )

        # -------------------------------------------------
        # WELCOME
        # -------------------------------------------------

        elif message_type == WELCOME:

            await self.handle_welcome(
                message,
                addr,
            )

        # -------------------------------------------------
        # GOSSIP
        # -------------------------------------------------

        elif message_type == GOSSIP:

            await self.handle_gossip(
                message,
                addr,
            )

        # -------------------------------------------------
        # TASK
        # -------------------------------------------------

        elif message_type == TASK:

            await self.handle_task(
                message,
                addr,
            )

        # -------------------------------------------------
        # FIND NODE
        # -------------------------------------------------

        elif message_type == FIND_NODE:

            await self.handle_find_node(
                message,
                addr,
            )

        # -------------------------------------------------
        # FIND NODE RESPONSE
        # -------------------------------------------------

        elif message_type == FIND_NODE_RESPONSE:

            await self.handle_find_node_response(
                message,
                addr,
            )

        # -------------------------------------------------
        # STORE
        # -------------------------------------------------

        elif message_type == STORE:

            await self.handle_store(
                message,
                addr,
            )

        # -------------------------------------------------
        # STORE RESPONSE
        # -------------------------------------------------

        elif message_type == STORE_RESPONSE:

            await self.handle_store_response(
                message,
                addr,
            )

        # -------------------------------------------------
        # FIND VALUE
        # -------------------------------------------------

        elif message_type == FIND_VALUE:

            await self.handle_find_value(
                message,
                addr,
            )

        # -------------------------------------------------
        # FIND VALUE RESPONSE
        # -------------------------------------------------

        elif message_type == FIND_VALUE_RESPONSE:

            await self.handle_find_value_response(
                message,
                addr,
            )

        # -------------------------------------------------
        # UNKNOWN MESSAGE
        # -------------------------------------------------

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

        if peer_id == self.node_id:
            return

        is_new_peer = (
            addr not in self.peers
        )

        self.peers.add(
            addr
        )

        print(
            f"\n[{self.node_id}] "
            f"HELLO received from "
            f"{peer_id} at {addr}"
        )

        # Add peer to DHT routing table
        self.add_peer_to_routing_table(
            peer_id,
            addr[0],
            addr[1],
        )

        if is_new_peer:

            print(
                f"[{self.node_id}] "
                f"Discovered peer: "
                f"{peer_id}"
            )

            self.print_peers()
            self.print_dht_table()

        # Create WELCOME response
        response = create_welcome(
            self.node_id,
            self.port,
        )

        try:

            self.transport.sendto(
                encode_message(response),
                addr,
            )

            print(
                f"[{self.node_id}] "
                f"WELCOME → {addr}"
            )

        except Exception as exc:

            print(
                f"[{self.node_id}] "
                f"WELCOME failed: {exc}"
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

        if peer_id == self.node_id:
            return

        is_new_peer = (
            addr not in self.peers
        )

        self.peers.add(
            addr
        )

        print(
            f"\n[{self.node_id}] "
            f"WELCOME received from "
            f"{peer_id} at {addr}"
        )

        # Add peer to DHT routing table
        self.add_peer_to_routing_table(
            peer_id,
            addr[0],
            addr[1],
        )

        if is_new_peer:

            print(
                f"[{self.node_id}] "
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

        if peer_id == self.node_id:
            return

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

        if not self.peer_loads:

            print(
                f"[{self.node_id}] "
                f"No peer load information "
                f"available."
            )

            return None

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
                f"{peer_id:15} "
                f"CPU: {cpu:5.1f}% | "
                f"RAM: {memory:5.1f}% | "
                f"SCORE: {score:5.1f}"
            )

        print("-" * 60)

    # =====================================================
    # ADD PEER TO DHT
    # =====================================================

    def add_peer_to_routing_table(
        self,
        peer_id,
        host,
        port,
    ):

        if not peer_id:
            return False

        if peer_id == self.node_id:
            return False

        try:

            peer_dht_id = generate_node_id(
                peer_id
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
    # FIND NODE
    # =====================================================

    async def handle_find_node(
        self,
        message,
        addr,
    ):

        requester_id = message.get(
            "node_id"
        )

        target_hex = message.get(
            "target_id"
        )

        if not target_hex:

            print(
                f"[{self.node_id}] "
                "FIND_NODE missing target."
            )

            return

        try:

            target_id = bytes.fromhex(
                target_hex
            )

        except ValueError:

            print(
                f"[{self.node_id}] "
                "Invalid FIND_NODE target."
            )

            return

        print(
            f"\n[{self.node_id}] "
            f"FIND_NODE request from "
            f"{requester_id}"
        )

        closest_peers = (
            self.routing_table.find_closest_peers(
                target_id,
                count=3,
            )
        )

        response_peers = []

        for peer in closest_peers:

            response_peers.append(
                {
                    "node_id": peer.node_id.hex(),
                    "host": peer.host,
                    "port": peer.port,
                }
            )

        response = (
            create_find_node_response(
                self.node_id,
                response_peers,
            )
        )

        self.transport.sendto(
            encode_message(response),
            addr,
        )

        print(
            f"[{self.node_id}] "
            f"FIND_NODE response → "
            f"{addr}"
        )

        print(
            f"   Returned peers: "
            f"{len(response_peers)}"
        )

    async def handle_find_node_response(
        self,
        message,
        addr,
    ):

        peer_id = message.get(
            "node_id"
        )

        peers = message.get(
            "peers",
            [],
        )

        print(
            f"\n[{self.node_id}] "
            f"FIND_NODE response from "
            f"{peer_id}"
        )

        if not peers:

            print(
                "   No peers returned."
            )

            return

        for peer in peers:

            print(
                f"   └── "
                f"{peer.get('host')}:"
                f"{peer.get('port')} "
                f"ID="
                f"{peer.get('node_id')}"
            )

            # Add returned peer to routing table
            returned_id = peer.get(
                "node_id"
            )

            returned_host = peer.get(
                "host"
            )

            returned_port = peer.get(
                "port"
            )

            if (
                returned_id
                and returned_host
                and returned_port
            ):

                try:

                    peer_dht_id = bytes.fromhex(
                        returned_id
                    )

                    peer_info = PeerInfo(
                        node_id=peer_dht_id,
                        host=returned_host,
                        port=returned_port,
                    )

                    self.routing_table.add_peer(
                        peer_info
                    )

                except Exception:
                    pass

    async def find_node(
        self,
        peer_addr,
        target_node_name,
    ):

        target_id = generate_node_id(
            target_node_name
        )

        message = create_find_node(
            self.node_id,
            target_id,
        )

        self.transport.sendto(
            encode_message(message),
            peer_addr,
        )

        print(
            f"\n[{self.node_id}] "
            f"FIND_NODE → {peer_addr}"
        )

        print(
            f"   Target: "
            f"{target_node_name}"
        )

        print(
            f"   Target DHT ID: "
            f"{node_id_to_hex(target_id)}"
        )

    # =====================================================
    # STORE
    # =====================================================

    async def handle_store(
        self,
        message,
        addr,
    ):
        """
        Store a key/value pair locally.
        """

        requester_id = message.get(
            "node_id"
        )

        key = message.get(
            "key"
        )

        value = message.get(
            "value"
        )

        if not key:

            print(
                f"[{self.node_id}] "
                "STORE request missing key."
            )

            return

        try:

            self.dht_storage.store(
                key,
                value,
            )

            success = True

            print(
                f"\n[{self.node_id}] "
                f"STORE received from "
                f"{requester_id}"
            )

            print(
                f"   Key   : {key}"
            )

            print(
                f"   Value : {value}"
            )

        except Exception as exc:

            success = False

            print(
                f"[{self.node_id}] "
                f"STORE failed: {exc}"
            )

        response = create_store_response(
            self.node_id,
            key,
            success,
        )

        self.transport.sendto(
            encode_message(response),
            addr,
        )

        print(
            f"[{self.node_id}] "
            f"STORE response → {addr}"
        )

    # =====================================================
    # STORE VALUE REMOTELY
    # =====================================================

    async def store_value(
        self,
        peer_addr,
        key,
        value,
    ):
        """
        Ask a remote peer to store a key/value pair.
        """

        message = create_store(
            self.node_id,
            key,
            value,
        )

        self.transport.sendto(
            encode_message(message),
            peer_addr,
        )

        print(
            f"\n[{self.node_id}] "
            f"STORE → {peer_addr}"
        )

        print(
            f"   Key   : {key}"
        )

        print(
            f"   Value : {value}"
        )

    # =====================================================
    # STORE RESPONSE
    # =====================================================

    async def handle_store_response(
        self,
        message,
        addr,
    ):

        peer_id = message.get(
            "node_id"
        )

        key = message.get(
            "key"
        )

        success = message.get(
            "success",
            False,
        )

        print(
            f"\n[{self.node_id}] "
            f"STORE response from "
            f"{peer_id}"
        )

        print(
            f"   Key     : {key}"
        )

        print(
            f"   Success : {success}"
        )

    # =====================================================
    # FIND VALUE
    # =====================================================

    async def handle_find_value(
        self,
        message,
        addr,
    ):
        """
        Look for a value in local DHT storage.

        If found, return the value.

        If not found, return the closest known peers.
        """

        requester_id = message.get(
            "node_id"
        )

        key = message.get(
            "key"
        )

        if not key:

            print(
                f"[{self.node_id}] "
                "FIND_VALUE missing key."
            )

            return

        print(
            f"\n[{self.node_id}] "
            f"FIND_VALUE request from "
            f"{requester_id}"
        )

        try:

            value = self.dht_storage.get(
                key
            )

        except Exception:

            value = None

        # -------------------------------------------------
        # VALUE FOUND
        # -------------------------------------------------

        if value is not None:

            print(
                f"[{self.node_id}] "
                f"Value found locally."
            )

            print(
                f"   Key   : {key}"
            )

            print(
                f"   Value : {value}"
            )

            response = create_find_value_response(
                self.node_id,
                key,
                value,
                [],
            )

        # -------------------------------------------------
        # VALUE NOT FOUND
        # -------------------------------------------------

        else:

            print(
                f"[{self.node_id}] "
                f"Value not found locally."
            )

            try:

                target_id = generate_node_id(
                    key
                )

                closest_peers = (
                    self.routing_table.find_closest_peers(
                        target_id,
                        count=3,
                    )
                )

            except Exception:

                closest_peers = []

            response_peers = []

            for peer in closest_peers:

                response_peers.append(
                    {
                        "node_id": peer.node_id.hex(),
                        "host": peer.host,
                        "port": peer.port,
                    }
                )

            response = create_find_value_response(
                self.node_id,
                key,
                None,
                response_peers,
            )

            print(
                f"   Returning "
                f"{len(response_peers)} "
                f"closest peers."
            )

        self.transport.sendto(
            encode_message(response),
            addr,
        )

        print(
            f"[{self.node_id}] "
            f"FIND_VALUE response → "
            f"{addr}"
        )

    # =====================================================
    # FIND VALUE RESPONSE
    # =====================================================

    async def handle_find_value_response(
        self,
        message,
        addr,
    ):

        peer_id = message.get(
            "node_id"
        )

        key = message.get(
            "key"
        )

        value = message.get(
            "value"
        )

        peers = message.get(
            "peers",
            [],
        )

        print(
            f"\n[{self.node_id}] "
            f"FIND_VALUE response from "
            f"{peer_id}"
        )

        print(
            f"   Key : {key}"
        )

        # -------------------------------------------------
        # VALUE FOUND
        # -------------------------------------------------

        if value is not None:

            print(
                f"   Value found: "
                f"{value}"
            )

            return value

        # -------------------------------------------------
        # VALUE NOT FOUND
        # -------------------------------------------------

        print(
            "   Value not found."
        )

        if not peers:

            print(
                "   No additional peers returned."
            )

            return None

        print(
            "   Closest peers:"
        )

        for peer in peers:

            print(
                f"   └── "
                f"{peer.get('host')}:"
                f"{peer.get('port')} "
                f"ID="
                f"{peer.get('node_id')}"
            )

        return None

    # =====================================================
    # FIND VALUE REMOTELY
    # =====================================================

    async def find_value(
        self,
        peer_addr,
        key,
    ):
        """
        Ask a remote peer for a value.
        """

        message = create_find_value(
            self.node_id,
            key,
        )

        self.transport.sendto(
            encode_message(message),
            peer_addr,
        )

        print(
            f"\n[{self.node_id}] "
            f"FIND_VALUE → {peer_addr}"
        )

        print(
            f"   Key: {key}"
        )

    # =====================================================
    # STORE LOCALLY
    # =====================================================

    def store_local(
        self,
        key,
        value,
    ):
        """
        Store a key/value pair directly
        in this node's DHT storage.
        """

        try:

            self.dht_storage.store(
                key,
                value,
            )

            print(
                f"\n[{self.node_id}] "
                f"Local DHT STORE"
            )

            print(
                f"   Key   : {key}"
            )

            print(
                f"   Value : {value}"
            )

            return True

        except Exception as exc:

            print(
                f"[{self.node_id}] "
                f"Local storage failed: "
                f"{exc}"
            )

            return False

    # =====================================================
    # GET LOCAL VALUE
    # =====================================================

    def get_local(
        self,
        key,
    ):
        """
        Retrieve a value from local DHT storage.
        """

        try:

            value = self.dht_storage.get(
                key
            )

            print(
                f"\n[{self.node_id}] "
                f"Local DHT GET"
            )

            print(
                f"   Key   : {key}"
            )

            print(
                f"   Value : {value}"
            )

            return value

        except Exception as exc:

            print(
                f"[{self.node_id}] "
                f"Local storage lookup failed: "
                f"{exc}"
            )

            return None

    # =====================================================
    # PRINT PEERS
    # =====================================================

    def print_peers(self):

        print()
        print(
            f"[{self.node_id}] "
            f"KNOWN PEERS"
        )

        print("-" * 60)

        if not self.peers:

            print(
                "No peers."
            )

        else:

            for host, port in self.peers:

                print(
                    f"   └── "
                    f"{host}:{port}"
                )

        print("-" * 60)

    # =====================================================
    # PRINT DHT ROUTING TABLE
    # =====================================================

    def print_dht_table(self):

        print()
        print(
            f"[{self.node_id}] "
            f"DHT ROUTING TABLE"
        )

        print("-" * 60)

        try:

            table = self.routing_table

            # If the routing table exposes buckets,
            # print their contents.

            buckets = getattr(
                table,
                "buckets",
                None,
            )

            if buckets is None:

                print(
                    "Routing table available."
                )

                print("-" * 60)

                return

            found = False

            for index, bucket in enumerate(
                buckets
            ):

                peers = getattr(
                    bucket,
                    "peers",
                    bucket,
                )

                if not peers:
                    continue

                found = True

                print(
                    f"Bucket {index}:"
                )

                for peer in peers:

                    peer_id = getattr(
                        peer,
                        "node_id",
                        None,
                    )

                    host = getattr(
                        peer,
                        "host",
                        "?",
                    )

                    port = getattr(
                        peer,
                        "port",
                        "?",
                    )

                    if isinstance(
                        peer_id,
                        bytes,
                    ):

                        peer_id_display = (
                            peer_id.hex()
                        )

                    else:

                        peer_id_display = str(
                            peer_id
                        )

                    print(
                        f"   └── "
                        f"{host}:{port} "
                        f"ID={peer_id_display}"
                    )

            if not found:

                print(
                    "No routing-table peers."
                )

        except Exception as exc:

            print(
                f"Unable to display "
                f"routing table: {exc}"
            )

        print("-" * 60)

    # =====================================================
    # PRINT DHT STORAGE
    # =====================================================

    def print_dht_storage(self):

        print()
        print(
            f"[{self.node_id}] "
            f"DHT STORAGE"
        )

        print("-" * 60)

        try:

            # Try common storage attributes.
            data = getattr(
                self.dht_storage,
                "data",
                None,
            )

            if data is None:

                data = getattr(
                    self.dht_storage,
                    "storage",
                    None,
                )

            if data is None:

                print(
                    "DHT storage is active."
                )

            elif not data:

                print(
                    "Storage is empty."
                )

            else:

                for key, value in data.items():

                    print(
                        f"   {key} → {value}"
                    )

        except Exception as exc:

            print(
                f"Unable to display "
                f"DHT storage: {exc}"
            )

        print("-" * 60)


# =========================================================
# NODE FACTORY
# =========================================================

def create_node(
    host,
    port,
    node_id,
    bootstrap_peers=None,
):
    """
    Convenience function for creating a MeshWeaver node.
    """

    return MeshNode(
        host=host,
        port=port,
        node_id=node_id,
        bootstrap_peers=bootstrap_peers,
    )