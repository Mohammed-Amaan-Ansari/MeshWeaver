import asyncio

from meshweaver.node import MeshNode

from meshweaver.task.model import Task

from meshweaver.network.transport import (
    start_udp_server,
)


def add(
    a,
    b,
):

    return a + b


async def main():

    node = MeshNode(
        host="127.0.0.1",
        port=9003,
        node_id="TASK_SENDER",
    )

    # Start UDP server
    await start_udp_server(
        node
    )

    await asyncio.sleep(1)

    # Create task
    task = Task(
        function_name="add",
        function=add,
        args=(10, 20),
    )

    print()
    print(
        "Creating remote task..."
    )

    print(
        f"Task ID : "
        f"{task.task_id}"
    )

    # Send task
    await node.send_task(
        task,
        ("127.0.0.1", 9002),
    )

    # Wait for result
    print()
    print(
        "Waiting for result..."
    )

    await asyncio.sleep(5)

    # Display stored result
    result = node.task_results.get(
        task.task_id
    )

    if result:

        print()
        print(
            "FINAL RESULT"
        )

        print(
            f"Status : "
            f"{result['status']}"
        )

        print(
            f"Result : "
            f"{result['result']}"
        )

    else:

        print(
            "No result received."
        )


if __name__ == "__main__":

    asyncio.run(main())