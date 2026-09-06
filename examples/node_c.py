import asyncio

from meshweaver.node import (
    MeshNode,
)


async def main():

    node = MeshNode(
        host="127.0.0.1",
        port=9003,
        node_id="NODE_C",

        bootstrap_peers=[
            ("127.0.0.1", 9001),
        ],
    )

    await node.start()


if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print(
            "\nNODE_C stopped."
        )