import asyncio
import time

from meshweaver.network.transport import (
    start_udp_server,
)

from meshweaver.scheduler.load_balancer import (
    select_best_peer,
)

from meshweaver.network.discovery import (
    HELLO,
    WELCOME,
    TASK,
    RESULT,
    GOSSIP,
    HEARTBEAT,
    create_hello,
    create_welcome,
    create_result,
    encode_message,
    decode_message,
)

from meshweaver.network.gossip import (
    gossip_loop,
)

from meshweaver.network.heartbeat import (
    heartbeat_loop,
    failure_detection_loop,
    mark_peer_alive,
)

from meshweaver.task.network import (
    create_task_message,
    extract_task,
)

from meshweaver.task.executor import (
    execute_task,
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

        # =====================================================
        # UDP TRANSPORT
        # =====================================================

        self.transport = None

        # =====================================================
        # PEERS
        # =====================================================

        # Known peer addresses
        self.peers = set()

        # =====================================================
        # PEER LOAD INFORMATION
        # =====================================================

        # Example:
        #
        # {
        #     "NODE_B": {
        #         "cpu": 20.5,
        #         "memory": 40.2
        #     }
        # }
        #
        self.peer_loads = {}

        # =====================================================
        # BOOTSTRAP PEERS
        # =====================================================

        self.bootstrap_peers = (
            bootstrap_peers or []
        )

        # =====================================================
        # TASK RESULTS
        # =====================================================

        self.task_results = {}

        # =====================================================
        # HEARTBEAT TRACKING
        # =====================================================

        # Last time a heartbeat was received.
        #
        # {
        #     "NODE_B": 1724470000.12
        # }
        #
        self.peer_last_seen = {}

        # Peer ID -> network address
        #
        # {
        #     "NODE_B": ("127.0.0.1", 9002)
        # }
        #
        self.peer_addresses = {}

        # Peers currently considered offline
        self.dead_peers = set()

    # =========================================================
    # START NODE
    # =========================================================

    async def start(self):

        print("=" * 50)
        print("Starting MeshWeaver Node")
        print(
            f"Node ID : {self.node_id}"
        )
        print(
            f"Address : "
            f"{self.host}:{self.port}"
        )
        print("=" * 50)

        # -----------------------------------------------------
        # Start UDP server
        # -----------------------------------------------------

        await start_udp_server(
            self
        )

        # Give the server time to start
        await asyncio.sleep(1)

        # -----------------------------------------------------
        # Initial peer discovery
        # -----------------------------------------------------

        await self.discover_peers()

        # -----------------------------------------------------
        # Periodic peer discovery
        # -----------------------------------------------------

        asyncio.create_task(
            self.discovery_loop()
        )

        # -----------------------------------------------------
        # CPU/RAM gossip
        # -----------------------------------------------------

        asyncio.create_task(
            gossip_loop(self)
        )

        # -----------------------------------------------------
        # Heartbeat sender
        # -----------------------------------------------------

        asyncio.create_task(
            heartbeat_loop(self)
        )

        # -----------------------------------------------------
        # Failure detector
        # -----------------------------------------------------

        asyncio.create_task(
            failure_detection_loop(self)
        )

        # -----------------------------------------------------
        # Keep node alive
        # -----------------------------------------------------

        try:

            await asyncio.Event().wait()

        except asyncio.CancelledError:

            print(
                f"\n[{self.node_id}] "
                "Node stopped."
            )

            raise

    # =========================================================
    # PEER DISCOVERY
    # =========================================================

    async def discover_peers(self):

        if self.transport is None:

            return

        message = create_hello(
            self.node_id,
            self.port,
        )

        data = encode_message(
            message
        )

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

    # =========================================================
    # MESSAGE HANDLING
    # =========================================================

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

        # -----------------------------------------------------
        # HELLO
        # -----------------------------------------------------

        if message_type == HELLO:

            await self.handle_hello(
                message,
                addr,
            )

        # -----------------------------------------------------
        # WELCOME
        # -----------------------------------------------------

        elif message_type == WELCOME:

            await self.handle_welcome(
                message,
                addr,
            )

        # -----------------------------------------------------
        # GOSSIP
        # -----------------------------------------------------

        elif message_type == GOSSIP:

            await self.handle_gossip(
                message,
                addr,
            )

        # -----------------------------------------------------
        # TASK
        # -----------------------------------------------------

        elif message_type == TASK:

            await self.handle_task(
                message,
                addr,
            )

        # -----------------------------------------------------
        # RESULT
        # -----------------------------------------------------

        elif message_type == RESULT:

            await self.handle_result(
                message,
                addr,
            )

        # -----------------------------------------------------
        # HEARTBEAT
        # -----------------------------------------------------

        elif message_type == HEARTBEAT:

            await self.handle_heartbeat(
                message,
                addr,
            )

        # -----------------------------------------------------
        # UNKNOWN
        # -----------------------------------------------------

        else:

            print(
                f"[{self.node_id}] "
                f"Unknown message type: "
                f"{message_type}"
            )

    # =========================================================
    # HELLO
    # =========================================================

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

        # Add peer
        self.peers.add(
            addr
        )

        # Save peer address
        self.peer_addresses[
            peer_id
        ] = addr

        # Consider peer alive
        self.peer_last_seen[
            peer_id
        ] = time.time()

        # If previously dead, mark alive
        if peer_id in self.dead_peers:

            self.dead_peers.remove(
                peer_id
            )

            print(
                f"\n[{self.node_id}] "
                f"PEER BACK ONLINE: "
                f"{peer_id}"
            )

        if is_new_peer:

            print(
                f"\n[{self.node_id}] "
                f"Discovered peer: "
                f"{peer_id}"
            )

            self.print_peers()

        # -----------------------------------------------------
        # Send WELCOME response
        # -----------------------------------------------------

        response = create_welcome(
            self.node_id,
            self.port,
        )

        self.transport.sendto(
            encode_message(
                response
            ),
            addr,
        )

    # =========================================================
    # WELCOME
    # =========================================================

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

        # Add peer
        self.peers.add(
            addr
        )

        # Save peer address
        self.peer_addresses[
            peer_id
        ] = addr

        # Consider peer alive
        self.peer_last_seen[
            peer_id
        ] = time.time()

        # Remove from dead peers
        if peer_id in self.dead_peers:

            self.dead_peers.remove(
                peer_id
            )

            print(
                f"\n[{self.node_id}] "
                f"PEER BACK ONLINE: "
                f"{peer_id}"
            )

        if is_new_peer:

            print(
                f"\n[{self.node_id}] "
                f"Connected with peer: "
                f"{peer_id}"
            )

            self.print_peers()

    # =========================================================
    # HEARTBEAT
    # =========================================================

    async def handle_heartbeat(
        self,
        message,
        addr,
    ):

        peer_id = message.get(
            "node_id"
        )

        # Ignore our own heartbeat
        if peer_id == self.node_id:

            return

        # Update heartbeat information
        mark_peer_alive(
            self,
            peer_id,
            addr,
        )

        print(
            f"[{self.node_id}] "
            f"HEARTBEAT ← "
            f"{peer_id}"
        )

    # =========================================================
    # GOSSIP
    # =========================================================

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

        # Validate load information
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

        # Validate CPU/RAM
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

    # =========================================================
    # LOAD BALANCER
    # =========================================================

    def get_best_peer(self):

        if not self.peer_loads:

            print(
                f"[{self.node_id}] "
                "No peer load information "
                "available."
            )

            return None

        # Remove offline peers
        available_loads = {
            peer_id: load
            for peer_id, load
            in self.peer_loads.items()
            if peer_id not in self.dead_peers
        }

        if not available_loads:

            print(
                f"[{self.node_id}] "
                "No online peers available."
            )

            return None

        best_peer = select_best_peer(
            available_loads
        )

        print(
            f"[{self.node_id}] "
            f"Best peer: "
            f"{best_peer}"
        )

        return best_peer

    def print_peer_loads(self):

        print()

        print(
            f"[{self.node_id}] "
            f"PEER LOAD TABLE"
        )

        print("-" * 70)

        if not self.peer_loads:

            print(
                "No peer load information."
            )

            print("-" * 70)

            return

        for peer_id, load in (
            self.peer_loads.items()
        ):

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

            status = (
                "OFFLINE"
                if peer_id
                in self.dead_peers
                else "ONLINE"
            )

            print(
                f"{peer_id:10} "
                f"CPU: {cpu:5.1f}% | "
                f"RAM: {memory:5.1f}% | "
                f"SCORE: {score:5.1f} | "
                f"{status}"
            )

        print("-" * 70)

    # =========================================================
    # SEND TASK
    # =========================================================

    async def send_task(
        self,
        task,
        peer_addr,
    ):

        if self.transport is None:

            raise RuntimeError(
                "Node UDP transport "
                "is not running."
            )

        # Mark task as assigned
        task.assign(
            f"{peer_addr[0]}:"
            f"{peer_addr[1]}"
        )

        # Create task message
        message = create_task_message(
            self.node_id,
            task,
        )

        data = encode_message(
            message
        )

        # Send task
        self.transport.sendto(
            data,
            peer_addr,
        )

        print()

        print("=" * 50)

        print(
            f"[{self.node_id}] "
            f"TASK SENT"
        )

        print("=" * 50)

        print(
            f"Task ID  : "
            f"{task.task_id}"
        )

        print(
            f"Function : "
            f"{task.function_name}"
        )

        print(
            f"Target   : "
            f"{peer_addr}"
        )

        print("=" * 50)

    # =========================================================
    # RECEIVE TASK
    # =========================================================

    async def handle_task(
        self,
        message,
        addr,
    ):

        try:

            task = extract_task(
                message
            )

        except Exception as exc:

            print(
                f"[{self.node_id}] "
                f"Failed to deserialize "
                f"task: {exc}"
            )

            return

        sender_id = message.get(
            "sender_id"
        )

        print()

        print("=" * 50)

        print(
            f"[{self.node_id}] "
            f"TASK RECEIVED"
        )

        print("=" * 50)

        print(
            f"Task ID      : "
            f"{task.task_id}"
        )

        print(
            f"Function     : "
            f"{task.function_name}"
        )

        print(
            f"Arguments    : "
            f"{task.args}"
        )

        print(
            f"Sender       : "
            f"{sender_id}"
        )

        print("=" * 50)

        # -----------------------------------------------------
        # Execute task
        # -----------------------------------------------------

        print(
            f"[{self.node_id}] "
            f"Executing task..."
        )

        task = execute_task(
            task
        )

        print()

        print(
            f"[{self.node_id}] "
            f"TASK FINISHED"
        )

        print(
            f"Status : "
            f"{task.status.value}"
        )

        if (
            task.status.value
            == "COMPLETED"
        ):

            print(
                f"Result : "
                f"{task.result}"
            )

        else:

            print(
                f"Error  : "
                f"{task.error}"
            )

        print("=" * 50)

        # -----------------------------------------------------
        # Send result back
        # -----------------------------------------------------

        result_message = create_result(
            sender_id=self.node_id,
            task_id=task.task_id,
            status=task.status.value,
            result=task.result,
            error=task.error,
        )

        self.transport.sendto(
            encode_message(
                result_message
            ),
            addr,
        )

        print(
            f"[{self.node_id}] "
            f"RESULT SENT → "
            f"{addr}"
        )

    # =========================================================
    # RECEIVE RESULT
    # =========================================================

    async def handle_result(
        self,
        message,
        addr,
    ):

        task_id = message.get(
            "task_id"
        )

        status = message.get(
            "status"
        )

        result = message.get(
            "result"
        )

        error = message.get(
            "error"
        )

        sender_id = message.get(
            "sender_id"
        )

        # Store result
        self.task_results[
            task_id
        ] = {
            "status": status,
            "result": result,
            "error": error,
            "sender_id": sender_id,
            "address": addr,
        }

        print()

        print("=" * 50)

        print(
            f"[{self.node_id}] "
            f"TASK RESULT RECEIVED"
        )

        print("=" * 50)

        print(
            f"Task ID : "
            f"{task_id}"
        )

        print(
            f"From    : "
            f"{sender_id}"
        )

        print(
            f"Status  : "
            f"{status}"
        )

        if status == "COMPLETED":

            print(
                f"Result  : "
                f"{result}"
            )

        else:

            print(
                f"Error   : "
                f"{error}"
            )

        print("=" * 50)

    # =========================================================
    # PEER TABLE
    # =========================================================

    def print_peers(self):

        print(
            f"[{self.node_id}] "
            f"Known peers: "
            f"{len(self.peers)}"
        )

        for host, port in self.peers:

            print(
                f"   └── "
                f"{host}:{port}"
            )