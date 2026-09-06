import asyncio

from meshweaver.node import (
    MeshNode,
)


# Week 4 Day 1 development security key
SECURITY_KEY = b"12345678901234567890123456789012"


async def main():

    node = MeshNode(
        host="127.0.0.1",
        port=9002,
        node_id="NODE_B",

        bootstrap_peers=[
            ("127.0.0.1", 9001),
        ],

        security_key=SECURITY_KEY,
    )

    await node.start()


if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print(
            "\nNODE_B stopped."
        )