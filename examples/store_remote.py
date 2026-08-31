import asyncio

from meshweaver.node import MeshNode


async def main():

    node = MeshNode(
        host="127.0.0.1",
        port=9006,
        node_id="STORE_CLIENT",
        bootstrap_peers=[
            ("127.0.0.1", 9001),
        ],
    )

    asyncio.create_task(
        node.start()
    )

    await asyncio.sleep(3)

    key = "task:remote:001"

    value = {
        "task_id": "remote:001",
        "operation": "compute",
        "status": "pending",
    }

    print()
    print("=" * 60)
    print("REMOTE DHT STORE TEST")
    print("=" * 60)

    await node.store_value(
        ("127.0.0.1", 9001),
        key,
        value,
    )

    print()
    print(
        "STORE request sent."
    )

    await asyncio.sleep(3)

    print()
    print(
        "Check NODE_A terminal for STORE request."
    )

    await asyncio.Event().wait()


if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:

        print(
            "\nSTORE_CLIENT stopped."
        )