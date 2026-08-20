import asyncio

from meshweaver.node import MeshNode

from meshweaver.task.model import Task

from meshweaver.network.transport import (
    start_udp_server,
)


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
        args=(10, 20),
    )

    # Send task to Node B
    await node.send_task(
        task,
        ("127.0.0.1", 9002),
    )

    # Give UDP time to send
    await asyncio.sleep(2)


if __name__ == "__main__":

    asyncio.run(main())