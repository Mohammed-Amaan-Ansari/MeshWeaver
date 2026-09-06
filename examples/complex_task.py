import asyncio

from meshweaver.node import (
    MeshNode,
)

from meshweaver.task.model import (
    Task,
)


def complex_math(
    numbers,
    power,
):

    squared = [
        value ** power
        for value in numbers
    ]

    total = sum(
        squared
    )

    average = (
        total / len(squared)
    )

    return {
        "input": numbers,
        "power": power,
        "values": squared,
        "sum": total,
        "average": average,
    }


async def main():

    node = MeshNode(
        host="127.0.0.1",
        port=9010,
        node_id="ML_SUBMITTER",

        bootstrap_peers=[
            ("127.0.0.1", 9002),
            ("127.0.0.1", 9003),
        ],
    )

    await node.start_components()

    await asyncio.sleep(7)

    task = Task(
        function_name="complex_math",
        function=complex_math,
        args=(
            [1, 2, 3, 4, 5],
        ),
        kwargs={
            "power": 3,
        },
    )

    print(
        "\nSubmitting complex function..."
    )

    await node.submit_task(
        task
    )

    result = await node.wait_for_task(
        task.task_id,
        timeout=30,
    )

    if result:

        print()
        print(
            "STATUS:",
            result.status.value
        )

        print(
            "RESULT:",
            result.result
        )

    await node.stop()


if __name__ == "__main__":

    asyncio.run(
        main()
    )