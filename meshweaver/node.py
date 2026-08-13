from dataclasses import dataclass, field
import asyncio
import uuid
import json

from meshweaver.network.transport import start_udp_server

@dataclass
class MeshNode:
    host: str
    port: int
    node_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    peers: set[tuple[str, int]] = field(default_factory=set)

    async def handle_message(self, message, addr):
        msg_type = message.get("type")

        if msg_type == "PING":
            print(f"📨 PING received from {addr}")

            response = {
                "type": "PONG",
                "from": self.node_id[:8],
            }

            self.transport.sendto(
                json.dumps(response).encode(),
                addr
            )

        elif msg_type == "PONG":
            print(f"✅ PONG received from {message.get('from')} ({addr})")

    async def send_ping(self, peer_host, peer_port):
        await asyncio.sleep(2)

        ping = {
            "type": "PING",
            "from": self.node_id[:8],
        }

        self.transport.sendto(
            json.dumps(ping).encode(),
            (peer_host, peer_port),
        )

        print(f"🚀 Sent PING to {peer_host}:{peer_port}")

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

        await asyncio.Event().wait()