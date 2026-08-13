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