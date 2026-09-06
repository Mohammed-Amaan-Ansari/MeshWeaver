import asyncio

from meshweaver.node import MeshNode
from meshweaver.security.config import SecurityConfig


async def main():

    security_config = SecurityConfig.development()

    node = MeshNode(
        host="127.0.0.1",
        port=9001,
        node_id="NODE_A",

        bootstrap_peers=[
            ("127.0.0.1", 9002),
        ],

        security_key=security_config.key,
    )

    await node.start()


if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        print("\nNODE_A stopped.")