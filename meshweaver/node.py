import asyncio
import time

from meshweaver.network.transport import (
    start_udp_server,
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

from meshweaver.scheduler.load_balancer import (
    select_best_peer,
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

        # Set of known peer addresses.
        #
        # Example:
        #
        # {
        #     ("127.0.0.1", 9002),
        #     ("127.0.0.1", 9003)
        # }
        #
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

        # Completed task results.
        #
        # {
        #     "task-id": {
        #         "status": "COMPLETED",
        #         "result": 123,
        #         ...
        #     }
        # }
        #
        self.task_results = {}

        # =====================================================
        # PENDING TASKS
        # =====================================================

        # Tasks currently assigned to remote peers.
        #
        # {
        #     "task-id": {
        #         "task": task_object,
        #         "peer_id": "NODE_B",
        #         "peer_addr": ("127.0.0.1", 9002)
        #     }
        # }
        #
        self.pending_tasks = {}

        # =====================================================
        # HEARTBEAT / FAILURE DETECTION
        # =====================================================

        # Last heartbeat received from each peer.
        #
        # {
        #     "NODE_B": timestamp
        # }
        #
        self.peer_last_seen = {}

        # Peer ID -> address.
        #
        # {
        #     "NODE_B": ("127.0.0.1", 9002)
        # }
        #
        self.peer_addresses = {}

        # Peers that are currently considered offline.
        self.dead_peers = set()

    # =========================================================
    # START NODE
    # =========================================================

    async def start(self):

        print("=" * 60)
        print("Starting MeshWeaver Node")
        print(
            f"Node ID : {self.node_id}"
        )
        print(
            f"Address : "
            f"{self.host}:{self.port}"
        )
        print("=" * 60)

        # -----------------------------------------------------
        # Start UDP server
        # -----------------------------------------------------

        await start_udp_server(
            self
        )

        # Give the UDP server time to initialize.
        await asyncio.sleep(1)

        # -----------------------------------------------------
        # Initial peer discovery
        # -----------------------------------------------------

        await self.discover_peers()

        # -----------------------------------------------------
        # Background services
        # -----------------------------------------------------

        asyncio.create_task(
            self.discovery_loop()
        )

        asyncio.create_task(
            gossip_loop(self)
        )

        asyncio.create_task(
            heartbeat_loop(self)
        )

        asyncio.create_task(
            failure_detection_loop(self)
        )

        asyncio.create_task(
            self.task_reassignment_loop()
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

            # Do not send HELLO to ourselves.
            if peer == (
                self.host,
                self.port,
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
                    f"Failed to send HELLO "
                    f"to {peer}: {exc}"
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

        # Ignore ourselves.
        if peer_id == self.node_id:
            return

        is_new_peer = (
            addr not in self.peers
        )

        # Register peer.
        self.peers.add(
            addr
        )

        self.peer_addresses[
            peer_id
        ] = addr

        self.peer_last_seen[
            peer_id
        ] = time.time()

        # If peer was previously offline,
        # mark it online again.
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
        # Send WELCOME
        # -----------------------------------------------------

        response = create_welcome(
            self.node_id,
            self.port,
        )

        try:

            self.transport.sendto(
                encode_message(
                    response
                ),
                addr,
            )

        except Exception as exc:

            print(
                f"[{self.node_id}] "
                f"Failed to send WELCOME: "
                f"{exc}"
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

        # Ignore ourselves.
        if peer_id == self.node_id:
            return

        is_new_peer = (
            addr not in self.peers
        )

        self.peers.add(
            addr
        )

        self.peer_addresses[
            peer_id
        ] = addr

        self.peer_last_seen[
            peer_id
        ] = time.time()

        # Peer has responded, so it is alive.
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

        # Ignore our own heartbeat.
        if peer_id == self.node_id:
            return

        # Register the peer if necessary.
        self.peers.add(
            addr
        )

        # Update heartbeat state.
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

        # Ignore ourselves.
        if peer_id == self.node_id:
            return

        # Validate load.
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

        # Register peer.
        self.peers.add(
            addr
        )

        self.peer_addresses[
            peer_id
        ] = addr

        # Receiving gossip also proves
        # that the peer is alive.
        self.peer_last_seen[
            peer_id
        ] = time.time()

        if peer_id in self.dead_peers:

            self.dead_peers.remove(
                peer_id
            )

        # Store load.
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

        # Only online peers can receive tasks.
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

        print("-" * 75)

        if not self.peer_loads:

            print(
                "No peer load information."
            )

            print("-" * 75)

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
                if peer_id in self.dead_peers
                else "ONLINE"
            )

            print(
                f"{peer_id:12} "
                f"CPU: {cpu:5.1f}% | "
                f"RAM: {memory:5.1f}% | "
                f"SCORE: {score:5.1f} | "
                f"{status}"
            )

        print("-" * 75)

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

        # -----------------------------------------------------
        # Find peer ID
        # -----------------------------------------------------

        peer_id = None

        for known_peer_id, known_addr in (
            self.peer_addresses.items()
        ):

            if known_addr == peer_addr:

                peer_id = known_peer_id

                break

        # If address is unknown, still allow
        # sending but warn.
        if peer_id is None:

            print(
                f"[{self.node_id}] "
                f"Warning: peer ID not found "
                f"for {peer_addr}"
            )

        # -----------------------------------------------------
        # Do not send to known offline peer
        # -----------------------------------------------------

        if (
            peer_id is not None
            and peer_id in self.dead_peers
        ):

            print(
                f"[{self.node_id}] "
                f"Cannot send task to "
                f"offline peer "
                f"{peer_id}"
            )

            return False

        # -----------------------------------------------------
        # Assign task
        # -----------------------------------------------------

        task.assign(
            f"{peer_addr[0]}:"
            f"{peer_addr[1]}"
        )

        # -----------------------------------------------------
        # Create network message
        # -----------------------------------------------------

        message = create_task_message(
            self.node_id,
            task,
        )

        data = encode_message(
            message
        )

        # -----------------------------------------------------
        # Send
        # -----------------------------------------------------

        try:

            self.transport.sendto(
                data,
                peer_addr,
            )

        except Exception as exc:

            print(
                f"[{self.node_id}] "
                f"Failed to send task: "
                f"{exc}"
            )

            return False

        # -----------------------------------------------------
        # Track task
        # -----------------------------------------------------

        self.pending_tasks[
            task.task_id
        ] = {
            "task": task,
            "peer_id": peer_id,
            "peer_addr": peer_addr,
        }

        print()
        print("=" * 60)
        print(
            f"[{self.node_id}] "
            f"TASK SENT"
        )
        print("=" * 60)

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
            f"{peer_id}"
        )

        print(
            f"Address  : "
            f"{peer_addr}"
        )

        print("=" * 60)

        return True

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
        print("=" * 60)

        print(
            f"[{self.node_id}] "
            f"TASK RECEIVED"
        )

        print("=" * 60)

        print(
            f"Task ID   : "
            f"{task.task_id}"
        )

        print(
            f"Function  : "
            f"{task.function_name}"
        )

        print(
            f"Arguments : "
            f"{task.args}"
        )

        print(
            f"Sender    : "
            f"{sender_id}"
        )

        print("=" * 60)

        # -----------------------------------------------------
        # Execute
        # -----------------------------------------------------

        print(
            f"[{self.node_id}] "
            f"Executing task..."
        )

        try:

            task = execute_task(
                task
            )

        except Exception as exc:

            print(
                f"[{self.node_id}] "
                f"Task execution error: "
                f"{exc}"
            )

            # Try to report execution failure.
            try:

                result_message = create_result(
                    sender_id=self.node_id,
                    task_id=task.task_id,
                    status="FAILED",
                    result=None,
                    error=str(exc),
                )

                self.transport.sendto(
                    encode_message(
                        result_message
                    ),
                    addr,
                )

            except Exception as result_exc:

                print(
                    f"[{self.node_id}] "
                    f"Failed to send error "
                    f"result: "
                    f"{result_exc}"
                )

            return

        # -----------------------------------------------------
        # Display result
        # -----------------------------------------------------

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

        print("=" * 60)

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

        try:

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

        except Exception as exc:

            print(
                f"[{self.node_id}] "
                f"Failed to send result: "
                f"{exc}"
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

        # -----------------------------------------------------
        # Store result
        # -----------------------------------------------------

        self.task_results[
            task_id
        ] = {
            "status": status,
            "result": result,
            "error": error,
            "sender_id": sender_id,
            "address": addr,
        }

        # -----------------------------------------------------
        # Completed tasks are no longer pending.
        # -----------------------------------------------------

        if status == "COMPLETED":

            self.pending_tasks.pop(
                task_id,
                None,
            )

        # Failed tasks can also be removed here.
        #
        # Day 7 reassignment is primarily based
        # on peer failure rather than task execution
        # failure.
        #
        elif status == "FAILED":

            self.pending_tasks.pop(
                task_id,
                None,
            )

        print()
        print("=" * 60)

        print(
            f"[{self.node_id}] "
            f"TASK RESULT RECEIVED"
        )

        print("=" * 60)

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

        print("=" * 60)

    # =========================================================
    # FAILED TASK REASSIGNMENT
    # =========================================================

    async def reassign_failed_tasks(self):

        if not self.pending_tasks:

            return

        # Work on a snapshot so the dictionary
        # can safely change during reassignment.
        pending_snapshot = list(
            self.pending_tasks.items()
        )

        for task_id, task_info in (
            pending_snapshot
        ):

            peer_id = task_info.get(
                "peer_id"
            )

            # -------------------------------------------------
            # We can only reassign if the assigned
            # peer is known to be dead.
            # -------------------------------------------------

            if peer_id is None:

                continue

            if peer_id not in self.dead_peers:

                continue

            print()
            print("=" * 60)

            print(
                f"[{self.node_id}] "
                f"FAILED PEER DETECTED"
            )

            print(
                f"Task   : {task_id}"
            )

            print(
                f"Failed : {peer_id}"
            )

            print("=" * 60)

            # -------------------------------------------------
            # Find another online peer.
            # -------------------------------------------------

            new_peer_id = self.get_best_peer()

            if new_peer_id is None:

                print(
                    f"[{self.node_id}] "
                    "No online peer available "
                    "for task reassignment."
                )

                continue

            # Safety check.
            if (
                new_peer_id
                == peer_id
            ):

                print(
                    f"[{self.node_id}] "
                    f"Selected peer "
                    f"{new_peer_id} "
                    f"is the failed peer."
                )

                continue

            if (
                new_peer_id
                in self.dead_peers
            ):

                print(
                    f"[{self.node_id}] "
                    f"Selected peer "
                    f"{new_peer_id} "
                    f"is offline."
                )

                continue

            # -------------------------------------------------
            # Find address.
            # -------------------------------------------------

            new_peer_addr = (
                self.peer_addresses.get(
                    new_peer_id
                )
            )

            if new_peer_addr is None:

                print(
                    f"[{self.node_id}] "
                    f"No address known for "
                    f"{new_peer_id}."
                )

                continue

            # -------------------------------------------------
            # Get original task.
            # -------------------------------------------------

            task = task_info.get(
                "task"
            )

            if task is None:

                print(
                    f"[{self.node_id}] "
                    f"Task object missing for "
                    f"{task_id}."
                )

                # Remove broken pending entry.
                self.pending_tasks.pop(
                    task_id,
                    None,
                )

                continue

            print(
                f"[{self.node_id}] "
                f"REASSIGNING TASK"
            )

            print(
                f"   Task : "
                f"{task_id}"
            )

            print(
                f"   From : "
                f"{peer_id}"
            )

            print(
                f"   To   : "
                f"{new_peer_id}"
            )

            # -------------------------------------------------
            # Remove old assignment before sending.
            # send_task() creates the new assignment.
            # -------------------------------------------------

            self.pending_tasks.pop(
                task_id,
                None,
            )

            success = await self.send_task(
                task,
                new_peer_addr,
            )

            if success:

                print(
                    f"[{self.node_id}] "
                    f"TASK REASSIGNED "
                    f"SUCCESSFULLY"
                )

            else:

                # Restore the pending task if
                # reassignment failed.
                self.pending_tasks[
                    task_id
                ] = task_info

                print(
                    f"[{self.node_id}] "
                    f"TASK REASSIGNMENT FAILED"
                )

    # =========================================================
    # TASK REASSIGNMENT LOOP
    # =========================================================

    async def task_reassignment_loop(self):

        while True:

            await asyncio.sleep(3)

            try:

                await self.reassign_failed_tasks()

            except Exception as exc:

                print(
                    f"[{self.node_id}] "
                    f"Reassignment error: "
                    f"{exc}"
                )

    # =========================================================
    # PEER TABLE
    # =========================================================

    def print_peers(self):

        print(
            f"[{self.node_id}] "
            f"Known peers: "
            f"{len(self.peers)}"
        )

        if not self.peers:

            print(
                "   └── No peers"
            )

            return

        for host, port in sorted(
            self.peers
        ):

            print(
                f"   └── "
                f"{host}:{port}"
            )