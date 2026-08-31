import asyncio

from meshweaver.node import MeshNode


async def main():

    node = MeshNode(
        host="127.0.0.1",
        port=9005,
        node_id="STORE_NODE",
        bootstrap_peers=[
            ("127.0.0.1", 9001),
        ],
    )

    # Start the node
    asyncio.create_task(
        node.start()
    )

    # Give the node time to start
    await asyncio.sleep(3)

    print()
    print("=" * 60)
    print("DAY 7 - DHT STORAGE NETWORK TEST")
    print("=" * 60)

    # -------------------------------------------------
    # LOCAL DHT STORAGE TEST
    # -------------------------------------------------

    key = "task:001"

    value = {
        "task_id": "task:001",
        "name": "demo_task",
        "status": "pending",
        "priority": 1,
    }

    print()
    print("[1] Storing value locally")

    node.dht_storage.store(
        key,
        value,
    )

    print(
        f"Key   : {key}"
    )

    print(
        f"Value : {value}"
    )

    # -------------------------------------------------
    # VERIFY VALUE
    # -------------------------------------------------

    print()
    print("[2] Retrieving value")

    result = node.dht_storage.get(
        key
    )

    print(
        f"Result: {result}"
    )

    # -------------------------------------------------
    # VERIFY KEY
    # -------------------------------------------------

    print()
    print("[3] Checking key")

    exists = node.dht_storage.contains(
        key
    )

    print(
        f"Exists: {exists}"
    )

    # -------------------------------------------------
    # DISPLAY STORAGE
    # -------------------------------------------------

    print()
    print("[4] Current DHT storage")

    for stored_key, stored_value in (
        node.dht_storage.all_items().items()
    ):

        print(
            f"   {stored_key} → {stored_value}"
        )

    print()
    print("=" * 60)
    print("DHT STORAGE TEST COMPLETED")
    print("=" * 60)

    # Keep node alive
    await asyncio.Event().wait()


if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:

        print(
            "\nSTORE_NODE stopped."
        )