import asyncio
import time

from meshweaver.network.transport import (
    start_udp_server,
)

from meshweaver.network.discovery import (
    HELLO,
    WELCOME,
    AUTH_CHALLENGE,
    AUTH_RESPONSE,
    AUTH_SUCCESS,
    GOSSIP,
    HEARTBEAT,
    HEARTBEAT_ACK,
    TASK,
    RESULT,
    FIND_NODE,
    FIND_NODE_RESPONSE,
    STORE,
    STORE_RESPONSE,
    FIND_VALUE,
    FIND_VALUE_RESPONSE,

    create_hello,
    create_welcome,
    create_auth_challenge,
    create_auth_response,
    create_auth_success,

    create_task_message,
    create_result_message,

    create_find_node,
    create_find_node_response,

    create_store,
    create_store_response,

    create_find_value,
    create_find_value_response,

    encode_message,
    decode_message,
)

from meshweaver.network.gossip import (
    gossip_loop,
)

from meshweaver.network.heartbeat import (
    heartbeat_loop,
    failure_detection_loop,
    handle_heartbeat,
    handle_heartbeat_ack,
)

from meshweaver.dht.node_id import (
    generate_node_id,
    node_id_to_hex,
    hex_to_node_id,
)

from meshweaver.dht.routing_table import (
    PeerInfo,
    RoutingTable,
)

from meshweaver.dht.storage import (
    DHTStorage,
)

from meshweaver.scheduler.load_balancer import (
    select_best_peer,
)

from meshweaver.task.model import (
    Task,
    TaskStatus,
)

from meshweaver.task.network import (
    extract_task,
)

from meshweaver.task.serializer import (
    serialize_task,
)

from meshweaver.task.executor import (
    execute_task,
)

# =========================================================
# WEEK 4 - DAY 2 SECURITY CONFIGURATION
# =========================================================

from meshweaver.security.config import (
    SecurityConfig,
    SecurityConfigError,
)

from meshweaver.security.transport_security import (
    TransportSecurity,
    PeerAuthenticationError,
)


class MeshNode:

    def __init__(
        self,
        host,
        port,
        node_id,
        bootstrap_peers=None,
        security_key=None,
    ):

        self.host = host
        self.port = port
        self.node_id = node_id

        # =================================================
        # TRANSPORT SECURITY
        # =================================================

        """
        Week 4 Day 2:

        Security configuration is now centralized.

        Priority:

        1. Explicit security_key passed to MeshNode
        2. MESHWEAVER_SECURITY_KEY environment variable
        3. Security disabled if no key is configured

        HMAC-SHA256 provides:

            - Authentication
            - Message integrity

        HMAC does NOT provide encryption.
        """

        self.security_key = security_key

        self.security_enabled = False

        # -------------------------------------------------
        # If no key was explicitly supplied, try loading
        # the key from the environment.
        # -------------------------------------------------

        if self.security_key is None:

            try:

                security_config = (
                    SecurityConfig.from_environment()
                )

                self.security_key = (
                    security_config.key
                )

                self.security_enabled = True

            except SecurityConfigError:

                self.security_key = None
                self.security_enabled = False

        else:

            # -------------------------------------------------
            # Validate explicitly supplied key through
            # SecurityConfig as well.
            # -------------------------------------------------

            try:

                security_config = SecurityConfig(
                    self.security_key
                )

                self.security_key = (
                    security_config.key
                )

                self.security_enabled = True

            except (
                SecurityConfigError,
                TypeError,
            ) as exc:

                raise ValueError(
                    f"Invalid security configuration: "
                    f"{exc}"
                ) from exc

        # =================================================
        # PEER AUTHENTICATION - WEEK 4 DAY 3
        # =================================================

        self.security = None

        if self.security_enabled:
            self.security = TransportSecurity(
                self.security_key
            )

        self.authenticated_peers = set()
        self.pending_authentication = {}

        # =================================================
        # TRANSPORT
        # =================================================

        self.transport = None

        # =================================================
        # PEERS
        # =================================================

        self.peers = set()

        # peer_id -> (host, port)
        self.peer_addresses = {}

        # =================================================
        # LOAD INFORMATION
        # =================================================

        self.peer_loads = {}

        # =================================================
        # HEARTBEAT
        # =================================================

        self.peer_last_seen = {}

        self.dead_peers = set()

        # =================================================
        # BOOTSTRAP
        # =================================================

        self.bootstrap_peers = (
            bootstrap_peers or []
        )

        # =================================================
        # TASKS
        # =================================================

        # task_id -> Task
        self.tasks = {}

        # task_id -> asyncio.Future
        self.task_futures = {}

        # =================================================
        # DHT
        # =================================================

        self.dht_node_id = (
            generate_node_id(
                self.node_id
            )
        )

        self.routing_table = (
            RoutingTable(
                self.dht_node_id
            )
        )

        self.dht_storage = (
            DHTStorage()
        )

        # =================================================
        # SHUTDOWN
        # =================================================

        self.shutdown_event = (
            asyncio.Event()
        )

        self.background_tasks = []

    # =====================================================
    # START
    # =====================================================

    async def start(self):

        print("=" * 65)
        print("Starting MeshWeaver Node")

        print(
            f"Node ID : {self.node_id}"
        )

        print(
            f"Address : "
            f"{self.host}:{self.port}"
        )

        print(
            f"DHT ID  : "
            f"{node_id_to_hex(self.dht_node_id)}"
        )

        print("=" * 65)

        # -------------------------------------------------
        # SECURITY STATUS
        # -------------------------------------------------

        if self.security_enabled:

            print(
                f"[{self.node_id}] "
                f"Security configuration: "
                f"HMAC-SHA256 enabled"
            )

        else:

            print(
                f"[{self.node_id}] "
                f"Security configuration: "
                f"DISABLED"
            )

        # -------------------------------------------------
        # START UDP TRANSPORT
        # -------------------------------------------------

        await start_udp_server(
            self,
            security_key=self.security_key,
        )

        await asyncio.sleep(1)

        await self.discover_peers()

        self.background_tasks = [

            asyncio.create_task(
                self.discovery_loop()
            ),

            asyncio.create_task(
                gossip_loop(self)
            ),

            asyncio.create_task(
                heartbeat_loop(self)
            ),

            asyncio.create_task(
                failure_detection_loop(
                    self
                )
            ),
        ]

        print(
            f"[{self.node_id}] "
            f"Node started successfully."
        )

        try:

            await self.shutdown_event.wait()

        except asyncio.CancelledError:

            pass

        finally:

            await self.stop()

    # =====================================================
    # STOP
    # =====================================================

    async def stop(self):

        if self.shutdown_event.is_set() is False:

            self.shutdown_event.set()

        for task in self.background_tasks:

            task.cancel()

        if self.background_tasks:

            await asyncio.gather(
                *self.background_tasks,
                return_exceptions=True,
            )

        if self.transport is not None:

            self.transport.close()

            self.transport = None

        print(
            f"[{self.node_id}] "
            f"Node stopped."
        )

    # =====================================================
    # DISCOVERY
    # =====================================================

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
                    f"HELLO error: {exc}"
                )

    async def discovery_loop(self):

        while not self.shutdown_event.is_set():

            await asyncio.sleep(10)

            try:

                await self.discover_peers()

            except Exception as exc:

                print(
                    f"[{self.node_id}] "
                    f"Discovery error: {exc}"
                )

    # =====================================================
    # MESSAGE ROUTER
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

        elif message_type == AUTH_CHALLENGE:
            await self.handle_auth_challenge(
                message,
                addr,
            )

        elif message_type == AUTH_RESPONSE:
            await self.handle_auth_response(
                message,
                addr,
            )

        elif message_type == AUTH_SUCCESS:
            await self.handle_auth_success(
                message,
                addr,
            )

        elif message_type == GOSSIP:

            await self.handle_gossip(
                message,
                addr,
            )

        elif message_type == HEARTBEAT:

            await handle_heartbeat(
                self,
                message,
                addr,
            )

        elif message_type == HEARTBEAT_ACK:

            await handle_heartbeat_ack(
                self,
                message,
                addr,
            )

        elif message_type == TASK:

            await self.handle_task(
                message,
                addr,
            )

        elif message_type == RESULT:

            await self.handle_result(
                message,
                addr,
            )

        elif message_type == FIND_NODE:

            await self.handle_find_node(
                message,
                addr,
            )

        elif message_type == FIND_NODE_RESPONSE:

            await self.handle_find_node_response(
                message,
                addr,
            )

        elif message_type == STORE:

            await self.handle_store(
                message,
                addr,
            )

        elif message_type == STORE_RESPONSE:

            await self.handle_store_response(
                message,
                addr,
            )

        elif message_type == FIND_VALUE:

            await self.handle_find_value(
                message,
                addr,
            )

        elif message_type == FIND_VALUE_RESPONSE:

            await self.handle_find_value_response(
                message,
                addr,
            )

        else:

            print(
                f"[{self.node_id}] "
                f"Unknown message: "
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

        self.register_peer(
            peer_id,
            addr,
        )

        print(
            f"[{self.node_id}] "
            f"HELLO ← {peer_id} "
            f"at {addr}"
        )

        response = create_welcome(
            self.node_id,
            self.port,
        )

        self.transport.sendto(
            encode_message(response),
            addr,
        )

        if self.security_enabled:
            self.authenticate_peer(
                peer_id,
                addr,
            )

    # =====================================================
    # WELCOME
    # =====================================================

    # =====================================================
    # PEER AUTHENTICATION
    # =====================================================

    def authenticate_peer(
        self,
        peer_id,
        peer_addr,
    ):
        """Start HMAC challenge-response authentication."""
        if not self.security_enabled or self.security is None:
            return
        if peer_id in self.authenticated_peers:
            return
        if peer_id in self.pending_authentication:
            return

        challenge = self.security.create_peer_challenge()
        self.pending_authentication[peer_id] = challenge

        message = create_auth_challenge(
            self.node_id,
            challenge,
        )

        try:
            self.transport.sendto(
                encode_message(message),
                peer_addr,
            )
            print(
                f"[{self.node_id}] "
                f"AUTH_CHALLENGE → {peer_id}"
            )
        except Exception as exc:
            self.pending_authentication.pop(peer_id, None)
            print(
                f"[{self.node_id}] "
                f"AUTH_CHALLENGE error: {exc}"
            )

    async def handle_auth_challenge(
        self,
        message,
        addr,
    ):
        """Respond to a peer authentication challenge."""
        if not self.security_enabled or self.security is None:
            return

        peer_id = message.get("node_id")
        challenge_hex = message.get("challenge")

        if not peer_id or not challenge_hex:
            print(
                f"[{self.node_id}] "
                f"Invalid AUTH_CHALLENGE from {addr}"
            )
            return

        if peer_id == self.node_id:
            return

        try:
            challenge = bytes.fromhex(challenge_hex)
        except (TypeError, ValueError):
            print(
                f"[{self.node_id}] "
                f"Invalid authentication challenge from {peer_id}"
            )
            return

        self.register_peer(peer_id, addr)

        response = self.security.create_peer_response(
            challenge,
            self.node_id,
        )

        response_message = create_auth_response(
            self.node_id,
            challenge,
            response,
        )

        try:
            self.transport.sendto(
                encode_message(response_message),
                addr,
            )
            print(
                f"[{self.node_id}] "
                f"AUTH_RESPONSE → {peer_id}"
            )
        except Exception as exc:
            print(
                f"[{self.node_id}] "
                f"AUTH_RESPONSE error: {exc}"
            )

    async def handle_auth_response(
        self,
        message,
        addr,
    ):
        """Verify a peer's HMAC authentication response."""
        if not self.security_enabled or self.security is None:
            return

        peer_id = message.get("node_id")
        challenge_hex = message.get("challenge")
        response_hex = message.get("response")

        if not peer_id or not challenge_hex or not response_hex:
            print(
                f"[{self.node_id}] "
                f"Invalid AUTH_RESPONSE from {peer_id}"
            )
            return

        pending_challenge = self.pending_authentication.get(peer_id)
        if pending_challenge is None:
            print(
                f"[{self.node_id}] "
                f"Unexpected AUTH_RESPONSE from {peer_id}"
            )
            return

        try:
            challenge = bytes.fromhex(challenge_hex)
            response = bytes.fromhex(response_hex)
        except (TypeError, ValueError):
            self.pending_authentication.pop(peer_id, None)
            print(
                f"[{self.node_id}] "
                f"Invalid AUTH_RESPONSE encoding from {peer_id}"
            )
            return

        if challenge != pending_challenge:
            self.pending_authentication.pop(peer_id, None)
            print(
                f"[{self.node_id}] "
                f"Authentication challenge mismatch from {peer_id}"
            )
            return

        try:
            self.security.verify_peer_response(
                challenge,
                peer_id,
                response,
            )
        except PeerAuthenticationError as exc:
            self.pending_authentication.pop(peer_id, None)
            print(
                f"[{self.node_id}] "
                f"Peer authentication failed: {exc}"
            )
            return

        self.authenticated_peers.add(peer_id)
        self.pending_authentication.pop(peer_id, None)
        self.register_peer(peer_id, addr)

        print(
            f"[{self.node_id}] "
            f"PEER AUTHENTICATED: {peer_id}"
        )

        success_message = create_auth_success(self.node_id)
        try:
            self.transport.sendto(
                encode_message(success_message),
                addr,
            )
            print(
                f"[{self.node_id}] "
                f"AUTH_SUCCESS → {peer_id}"
            )
        except Exception as exc:
            print(
                f"[{self.node_id}] "
                f"AUTH_SUCCESS error: {exc}"
            )

    async def handle_auth_success(
        self,
        message,
        addr,
    ):
        """Mark the peer as authenticated after success."""
        if not self.security_enabled:
            return

        peer_id = message.get("node_id")
        if not peer_id or peer_id == self.node_id:
            return

        self.register_peer(peer_id, addr)
        self.authenticated_peers.add(peer_id)
        self.pending_authentication.pop(peer_id, None)

        print(
            f"[{self.node_id}] "
            f"PEER AUTHENTICATED: {peer_id}"
        )

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

        self.register_peer(
            peer_id,
            addr,
        )

        print(
            f"[{self.node_id}] "
            f"WELCOME ← {peer_id} "
            f"at {addr}"
        )

    # =====================================================
    # REGISTER PEER
    # =====================================================

    def register_peer(
        self,
        peer_id,
        addr,
    ):

        if peer_id == self.node_id:
            return

        self.peers.add(
            addr
        )

        self.peer_addresses[
            peer_id
        ] = addr

        self.peer_last_seen[
            peer_id
        ] = time.time()

        self.add_peer_to_routing_table(
            peer_id,
            addr[0],
            addr[1],
        )

        if peer_id in self.dead_peers:

            self.dead_peers.remove(
                peer_id
            )

            print(
                f"[{self.node_id}] "
                f"PEER BACK ONLINE: "
                f"{peer_id}"
            )

    # =====================================================
    # PEER ALIVE
    # =====================================================

    def mark_peer_alive(
        self,
        peer_id,
        addr,
    ):

        if peer_id == self.node_id:
            return

        self.register_peer(
            peer_id,
            addr,
        )

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
            return

        cpu = load.get(
            "cpu"
        )

        memory = load.get(
            "memory"
        )

        if cpu is None or memory is None:
            return

        self.register_peer(
            peer_id,
            addr,
        )

        self.peer_loads[
            peer_id
        ] = {
            "cpu": float(cpu),
            "memory": float(memory),
        }

        print(
            f"[{self.node_id}] "
            f"LOAD ← {peer_id} | "
            f"CPU={float(cpu):.1f}% | "
            f"RAM={float(memory):.1f}%"
        )

    # =====================================================
    # TASK SUBMISSION
    # =====================================================

    async def submit_task(
        self,
        task: Task,
    ):

        if not isinstance(
            task,
            Task,
        ):
            raise TypeError(
                "submit_task() expects "
                "a Task object"
            )

        self.tasks[
            task.task_id
        ] = task

        best_peer = (
            self.get_best_peer()
        )

        if best_peer is None:

            task.fail(
                "No healthy peer available."
            )

            print(
                f"[{self.node_id}] "
                f"No worker available."
            )

            return task

        return await self._assign_task(
            task,
            best_peer,
        )

    # =====================================================
    # SELECT BEST PEER
    # =====================================================

    def get_best_peer(
        self,
        excluded=None,
    ):

        excluded = excluded or set()

        available_loads = {}

        for peer_id, load in (
            self.peer_loads.items()
        ):

            if peer_id in excluded:
                continue

            if peer_id in self.dead_peers:
                continue

            if peer_id not in self.peer_addresses:
                continue

            available_loads[
                peer_id
            ] = load

        return select_best_peer(
            available_loads
        )

    # =====================================================
    # ASSIGN TASK
    # =====================================================

    async def _assign_task(
        self,
        task,
        peer_id,
    ):

        task.assign(
            peer_id
        )

        self.tasks[
            task.task_id
        ] = task

        peer_addr = (
            self.peer_addresses.get(
                peer_id
            )
        )

        if peer_addr is None:

            task.fail(
                "Peer address unavailable."
            )

            return task

        data = serialize_task(
            task
        )

        message = create_task_message(
            self.node_id,
            task.task_id,
            data,
        )

        try:

            self.transport.sendto(
                encode_message(message),
                peer_addr,
            )

            print()
            print(
                f"[{self.node_id}] "
                f"TASK ROUTING"
            )

            print(
                f"   Task      : "
                f"{task.task_id}"
            )

            print(
                f"   Selected  : "
                f"{peer_id}"
            )

            print(
                f"   Load      : "
                f"{self.peer_loads.get(peer_id)}"
            )

            print(
                f"   Address   : "
                f"{peer_addr}"
            )

        except Exception as exc:

            task.fail(
                str(exc)
            )

        return task

    # =====================================================
    # TASK RECEIVER
    # =====================================================

    async def handle_task(self, message, addr):
        """
    Handle an incoming TASK message.

    Only authenticated peers are allowed to submit tasks.
    """

        sender_id = message.get("sender_id")

    # ---------------------------------------------------------
    # Security check 1: sender_id must exist
    # ---------------------------------------------------------
        if not sender_id:
            print(
                f"[{self.node_id}] "
            f"SECURITY: rejected TASK with missing sender_id "
            f"from {addr}"
        )
        return

    # ---------------------------------------------------------
    # Security check 2: sender must be authenticated
    # ---------------------------------------------------------
        if sender_id not in self.authenticated_peers:
            print(
            f"[{self.node_id}] "
            f"SECURITY: rejected TASK from "
            f"unauthenticated peer {sender_id}"
        )
        return

    # ---------------------------------------------------------
    # Authorized task
    # ---------------------------------------------------------
        print(
        f"[{self.node_id}] "
        f"Authorized TASK received from {sender_id}"
    )

        task_id = message.get("task_id")
        task_data_hex = message.get("task_data")

        if not task_id:
            print(
            f"[{self.node_id}] "
            f"SECURITY: rejected TASK with missing task_id"
        )
        return

        if not task_data_hex:
            print(
            f"[{self.node_id}] "
            f"SECURITY: rejected TASK with missing task_data"
            )
        return

        try:
            task_data = bytes.fromhex(task_data_hex)
        except ValueError:
            print(
            f"[{self.node_id}] "
            f"SECURITY: rejected TASK with invalid task_data"
        )
        return

    # ---------------------------------------------------------
    # Existing task processing starts here
    # ---------------------------------------------------------
        try:
            await self.execute_task(
            sender_id=sender_id,
            task_id=task_id,
            task_data=task_data,
            addr=addr,
        )

        except Exception as exc:
            print(
            f"[{self.node_id}] "
            f"Task execution error: {exc}"
        )
            

    def is_peer_authenticated(self, peer_id):
        """
    Return True if the peer has completed authentication.
    """
        return peer_id in self.authenticated_peers
    # =====================================================
    # RESULT
    # =====================================================

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

        task = self.tasks.get(
            task_id
        )

        if task is None:

            print(
                f"[{self.node_id}] "
                f"Unknown task result: "
                f"{task_id}"
            )

            return

        if status == TaskStatus.COMPLETED.value:

            task.complete(
                result
            )

            print()
            print(
                f"[{self.node_id}] "
                f"TASK COMPLETED"
            )

            print(
                f"   Task   : "
                f"{task_id}"
            )

            print(
                f"   Worker : "
                f"{sender_id}"
            )

            print(
                f"   Result : "
                f"{result}"
            )

        else:

            task.fail(
                error or "Remote task failed."
            )

            print()
            print(
                f"[{self.node_id}] "
                f"TASK FAILED"
            )

            print(
                f"   Task   : "
                f"{task_id}"
            )

            print(
                f"   Worker : "
                f"{sender_id}"
            )

            print(
                f"   Error  : "
                f"{task.error}"
            )

        future = self.task_futures.get(
            task_id
        )

        if future and not future.done():

            future.set_result(
                task
            )

    # =====================================================
    # WAIT FOR RESULT
    # =====================================================

    async def wait_for_task(
        self,
        task_id,
        timeout=None,
    ):

        task = self.tasks.get(
            task_id
        )

        if task is None:
            return None

        if task.status in (
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
        ):
            return task

        loop = asyncio.get_running_loop()

        future = loop.create_future()

        self.task_futures[
            task_id
        ] = future

        try:

            if timeout:

                return await asyncio.wait_for(
                    future,
                    timeout=timeout,
                )

            return await future

        finally:

            self.task_futures.pop(
                task_id,
                None,
            )

    # =====================================================
    # PEER FAILURE
    # =====================================================

    async def handle_peer_failure(
        self,
        peer_id,
    ):

        addr = self.peer_addresses.get(
            peer_id
        )

        if addr:

            self.peers.discard(
                addr
            )

        self.peer_loads.pop(
            peer_id,
            None,
        )

        try:

            peer_dht_id = (
                generate_node_id(
                    peer_id
                )
            )

            self.routing_table.remove_peer(
                peer_dht_id
            )

        except Exception:
            pass

        # Find tasks assigned to dead peer.
        failed_tasks = []

        for task in self.tasks.values():

            if (
                task.assigned_peer
                == peer_id
                and task.status
                in (
                    TaskStatus.ASSIGNED,
                    TaskStatus.RUNNING,
                )
            ):

                failed_tasks.append(
                    task
                )

        if not failed_tasks:

            return

        print()
        print(
            f"[{self.node_id}] "
            f"FOUND {len(failed_tasks)} "
            f"TASK(S) AFFECTED BY "
            f"{peer_id} FAILURE"
        )

        for task in failed_tasks:

            task.fail(
                f"Worker {peer_id} "
                f"went offline."
            )

            print(
                f"   Task {task.task_id} "
                f"marked FAILED."
            )

            if task.can_retry():

                print(
                    f"   Re-routing "
                    f"{task.task_id}..."
                )

                excluded = {
                    peer_id
                }

                new_peer = (
                    self.get_best_peer(
                        excluded=excluded
                    )
                )

                if new_peer:

                    task.error = None

                    await self._assign_task(
                        task,
                        new_peer,
                    )

                else:

                    print(
                        f"   No healthy peer "
                        f"available for "
                        f"{task.task_id}."
                    )

    # =====================================================
    # DHT ROUTING
    # =====================================================

    def add_peer_to_routing_table(
        self,
        peer_id,
        host,
        port,
    ):

        peer_node_id = (
            generate_node_id(
                peer_id
            )
        )

        peer = PeerInfo(
            node_id=peer_node_id,
            host=host,
            port=port,
        )

        self.routing_table.add_peer(
            peer
        )

    async def handle_find_node(
        self,
        message,
        addr,
    ):

        target_hex = message.get(
            "target_id"
        )

        try:

            target_id = hex_to_node_id(
                target_hex
            )

        except Exception:

            return

        closest = (
            self.routing_table
            .find_closest_peers(
                target_id,
                count=3,
            )
        )

        peers = []

        for peer in closest:

            peers.append(
                {
                    "node_id":
                        peer.node_id.hex(),
                    "host":
                        peer.host,
                    "port":
                        peer.port,
                }
            )

        response = (
            create_find_node_response(
                self.node_id,
                peers,
            )
        )

        self.transport.sendto(
            encode_message(response),
            addr,
        )

        print(
            f"[{self.node_id}] "
            f"FIND_NODE → "
            f"{len(peers)} peers"
        )

    async def handle_find_node_response(
        self,
        message,
        addr,
    ):

        peers = message.get(
            "peers",
            [],
        )

        print()
        print(
            f"[{self.node_id}] "
            f"FIND_NODE RESPONSE"
        )

        for peer in peers:

            print(
                f"   └── "
                f"{peer.get('host')}:"
                f"{peer.get('port')} "
                f"ID="
                f"{peer.get('node_id')}"
            )

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

    # =====================================================
    # DHT STORE
    # =====================================================

    async def handle_store(
        self,
        message,
        addr,
    ):

        key = message.get(
            "key"
        )

        value = message.get(
            "value"
        )

        success = False

        try:

            success = (
                self.dht_storage.store(
                    key,
                    value,
                )
            )

        except Exception:

            success = False

        response = (
            create_store_response(
                self.node_id,
                key,
                success,
            )
        )

        self.transport.sendto(
            encode_message(response),
            addr,
        )

    async def handle_store_response(
        self,
        message,
        addr,
    ):

        print(
            f"[{self.node_id}] "
            f"STORE RESPONSE | "
            f"key={message.get('key')} | "
            f"success={message.get('success')}"
        )

    async def store_value(
        self,
        peer_addr,
        key,
        value,
    ):

        message = create_store(
            self.node_id,
            key,
            value,
        )

        self.transport.sendto(
            encode_message(message),
            peer_addr,
        )

    # =====================================================
    # START COMPONENTS
    # =====================================================

    async def start_components(self):

        if self.security_enabled:

            print(
                f"[{self.node_id}] "
                f"Security configuration: "
                f"HMAC-SHA256 enabled"
            )

        else:

            print(
                f"[{self.node_id}] "
                f"Security configuration: "
                f"DISABLED"
            )

        await start_udp_server(
            self,
            security_key=self.security_key,
        )

        await asyncio.sleep(1)

        await self.discover_peers()

        self.background_tasks = [

            asyncio.create_task(
                self.discovery_loop()
            ),

            asyncio.create_task(
                gossip_loop(self)
            ),

            asyncio.create_task(
                heartbeat_loop(self)
            ),

            asyncio.create_task(
                failure_detection_loop(
                    self
                )
            ),
        ]

        print(
            f"[{self.node_id}] "
            f"Components started."
        )

    # =====================================================
    # DHT FIND VALUE
    # =====================================================

    async def handle_find_value(
        self,
        message,
        addr,
    ):

        key = message.get(
            "key"
        )

        value = self.dht_storage.get(
            key
        )

        if value is not None:

            response = (
                create_find_value_response(
                    self.node_id,
                    key,
                    value=value,
                    found=True,
                )
            )

        else:

            closest = (
                self.routing_table
                .find_closest_peers(
                    generate_node_id(key),
                    count=3,
                )
            )

            peers = []

            for peer in closest:

                peers.append(
                    {
                        "node_id":
                            peer.node_id.hex(),
                        "host":
                            peer.host,
                        "port":
                            peer.port,
                    }
                )

            response = (
                create_find_value_response(
                    self.node_id,
                    key,
                    value=None,
                    found=False,
                    peers=peers,
                )
            )

        self.transport.sendto(
            encode_message(response),
            addr,
        )

    async def handle_find_value_response(
        self,
        message,
        addr,
    ):

        print(
            f"[{self.node_id}] "
            f"FIND_VALUE RESPONSE"
        )

        print(
            f"   Key   : "
            f"{message.get('key')}"
        )

        print(
            f"   Found : "
            f"{message.get('found')}"
        )

        if message.get("found"):

            print(
                f"   Value : "
                f"{message.get('value')}"
            )

        else:

            print(
                "   Value not found."
            )

    async def find_value(
        self,
        peer_addr,
        key,
    ):

        message = create_find_value(
            self.node_id,
            key,
        )

        self.transport.sendto(
            encode_message(message),
            peer_addr,
        )

    # =====================================================
    # LOCAL DHT
    # =====================================================

    def store_local(
        self,
        key,
        value,
    ):

        return self.dht_storage.store(
            key,
            value,
        )

    def get_local(
        self,
        key,
    ):

        return self.dht_storage.get(
            key
        )

    # =====================================================
    # DEBUG
    # =====================================================

    def print_peers(self):

        print()
        print(
            f"[{self.node_id}] "
            f"KNOWN PEERS"
        )

        for peer in sorted(
            self.peers
        ):

            peer_id = next(
                (
                    node_id
                    for node_id, address
                    in self.peer_addresses.items()
                    if address == peer
                ),
                "UNKNOWN",
            )

            auth_status = (
                "AUTHENTICATED"
                if peer_id in self.authenticated_peers
                else "NOT AUTHENTICATED"
            )

            print(
                f"   └── "
                f"{peer[0]}:{peer[1]} | "
                f"{peer_id} | "
                f"{auth_status}"
            )

    def print_loads(self):

        print()
        print(
            f"[{self.node_id}] "
            f"PEER LOADS"
        )

        for peer_id, load in (
            self.peer_loads.items()
        ):

            status = (
                "OFFLINE"
                if peer_id in self.dead_peers
                else "ONLINE"
            )

            print(
                f"   └── "
                f"{peer_id} | "
                f"CPU={load.get('cpu', 0):.1f}% | "
                f"RAM={load.get('memory', 0):.1f}% | "
                f"{status}"
            )

    def print_dht_table(self):

        print()
        print(
            f"[{self.node_id}] "
            f"DHT ROUTING TABLE"
        )

        peers = (
            self.routing_table
            .get_all_peers()
        )

        if not peers:

            print(
                "   No peers."
            )

            return

        for peer in peers:

            print(
                f"   └── "
                f"{peer.host}:"
                f"{peer.port} | "
                f"DHT={peer.node_id.hex()}"
            )

    def print_tasks(self):

        print()
        print(
            f"[{self.node_id}] "
            f"TASKS"
        )

        for task in self.tasks.values():

            print(
                f"   └── "
                f"{task.task_id} | "
                f"{task.status.value} | "
                f"worker={task.assigned_peer} | "
                f"attempts={task.attempts}"
            )


# =========================================================
# NODE FACTORY
# =========================================================

def create_node(
    host,
    port,
    node_id,
    bootstrap_peers=None,
    security_key=None,
):

    return MeshNode(
        host=host,
        port=port,
        node_id=node_id,
        bootstrap_peers=bootstrap_peers,
        security_key=security_key,
    )