from dataclasses import dataclass, field
import asyncio
import uuid
import json

from meshweaver.network.transport import start_udp_server
from meshweaver.network.discovery import discovery_loop


@dataclass
class MeshNode:
    host: str
    port: int
    bootstrap_peers: list[tuple[str, int]] = field(default_factory=list)

    node_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    peers: set[tuple[str, int]] = field(default_factory=set)

    async def handle_message(self, message, addr):
        msg_type = message.get("type")

        if msg_type == "HELLO":
            self.peers.add(addr)

            print(
                f"🟢 HELLO received from "
                f"{message.get('node_id')} {addr}"
            )

            response = {
                "type": "WELCOME",
                "node_id": self.node_id[:8],
                "port": self.port,
            }

            self.transport.sendto(
                json.dumps(response).encode(),
                addr,
            )

        elif msg_type == "WELCOME":
            self.peers.add(addr)

            print(
                f"🤝 Connected with peer "
                f"{message.get('node_id')} {addr}"
            )

            print(f"🔗 Current peers: {self.peers}")

    async def show_peers(self):
        while True:
            await asyncio.sleep(15)

            print(
                f"📋 [{self.node_id[:8]}] peers: "
                f"{list(self.peers)}"
            )

    async def start(self):
        self.transport = await start_udp_server(
            self.host,
            self.port,
            self.handle_message,
        )

        print(
            f"🌐 MeshNode {self.node_id[:8]} listening on "
            f"{self.host}:{self.port}"
        )

        # Start automatic discovery
        asyncio.create_task(
            discovery_loop(self, self.bootstrap_peers)
        )

        # Display peer table periodically
        asyncio.create_task(self.show_peers())

        # Keep node alive
        await asyncio.Event().wait()