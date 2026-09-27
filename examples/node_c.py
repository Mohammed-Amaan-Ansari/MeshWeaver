
import asyncio

from meshweaver.node import MeshNode
from meshweaver.security.config import SecurityConfig


async def main():
    config = SecurityConfig.development()

    node = MeshNode(
        host="127.0.0.1",
        port=9003,
        node_id="NODE_C",
        bootstrap_peers=[
            ("127.0.0.1", 9001),
            ("127.0.0.1", 9002),
        ],
        security_key=config.key,
    )

    await node.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("\nNODE_C stopped.")