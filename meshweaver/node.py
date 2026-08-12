from dataclasses import dataclass, field
import asyncio
import uuid


@dataclass
class MeshNode:
    host: str
    port: int
    node_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    peers: set[tuple[str, int]] = field(default_factory=set)

    async def start(self):
        print(f" Node started: {self.node_id[:8]} @ {self.host}:{self.port}")

        while True:
            await asyncio.sleep(5)

            print(
                f" [{self.node_id[:8]}] alive | peers={len(self.peers)}"
            )