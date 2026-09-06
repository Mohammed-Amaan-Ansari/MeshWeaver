import asyncio

from meshweaver.node import (
    MeshNode,
)

from meshweaver.task.model import (
    Task,
)


def calculate_sum(
    numbers,
):

    return sum(numbers)


async def main():

    node = MeshNode(
        host="127.0.0.1",
        port=9001,
        node_id="SUBMITTER",

        bootstrap_peers=[
            ("127.0.0.1", 9002),
            ("127.0.0.1", 9003),
        ],
    )

    # Start networking without
    # blocking this example forever.

    await node.start_components()

    await asyncio.sleep(7)

    node.print_peers()
    node.print_loads()

    task = Task(
        function_name="calculate_sum",
        function=calculate_sum,
        args=(
            [10, 20, 30, 40, 50],
        ),
    )

    print()
    print("=" * 60)
    print("SUBMITTING TASK")
    print("=" * 60)

    await node.submit_task(
        task
    )

    result = await node.wait_for_task(
        task.task_id,
        timeout=30,
    )

    print()
    print("=" * 60)
    print("FINAL TASK STATE")
    print("=" * 60)

    if result:

        print(
            f"Task ID : "
            f"{result.task_id}"
        )

        print(
            f"Status  : "
            f"{result.status.value}"
        )

        print(
            f"Worker  : "
            f"{result.assigned_peer}"
        )

        print(
            f"Result  : "
            f"{result.result}"
        )

        print(
            f"Error   : "
            f"{result.error}"
        )

    await node.stop()


if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print(
            "\nSubmitter stopped."
        )