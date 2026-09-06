import asyncio

from meshweaver.node import MeshNode
from meshweaver.security.config import SecurityConfig


async def main():

    security_config = SecurityConfig.development()

    node = MeshNode(
        host="127.0.0.1",
        port=9002,
        node_id="NODE_B",

        bootstrap_peers=[
            ("127.0.0.1", 9001),
        ],

        security_key=security_config.key,
    )

    await node.start()


if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        print("\nNODE_B stopped.")