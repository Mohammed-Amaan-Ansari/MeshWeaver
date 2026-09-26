import asyncio

from meshweaver.node import MeshNode
from meshweaver.task.model import Task
from meshweaver.security.config import SecurityConfig


def calculate_sum(numbers):
    return sum(numbers)


async def main():

    # =========================================================
    # SECURITY CONFIGURATION
    # =========================================================

    config = SecurityConfig.development()

    # =========================================================
    # CREATE SUBMITTER NODE
    # =========================================================

    node = MeshNode(
        host="127.0.0.1",
        port=9004,
        node_id="SUBMITTER",

        bootstrap_peers=[
            ("127.0.0.1", 9001),  # NODE_A
            ("127.0.0.1", 9002),  # NODE_B
        ],

        # Same security key used by the other nodes
        security_key=config.key,
    )

    # =========================================================
    # START NODE COMPONENTS
    # =========================================================

    print()
    print("=" * 60)
    print("Starting MeshWeaver Submitter")
    print("=" * 60)

    print("Node ID  : SUBMITTER")
    print("Address  : 127.0.0.1:9004")
    print("Security : ENABLED")

    await node.start_components()

    # Give the submitter time to discover and authenticate
    # with the other nodes.
    print()
    print("[SUBMITTER] Waiting for peer discovery...")

    await asyncio.sleep(7)

    # =========================================================
    # DISPLAY NETWORK STATE
    # =========================================================

    print()
    print("=" * 60)
    print("NETWORK STATE")
    print("=" * 60)

    node.print_peers()
    node.print_loads()

    # =========================================================
    # CREATE TASK
    # =========================================================

    task = Task(
        function_name="calculate_sum",
        function=calculate_sum,
        args=(
            [10, 20, 30, 40, 50],
        ),
    )

    # =========================================================
    # SUBMIT TASK
    # =========================================================

    print()
    print("=" * 60)
    print("SUBMITTING TASK")
    print("=" * 60)

    print(f"Task ID : {task.task_id}")
    print("Function: calculate_sum")
    print("Input   : [10, 20, 30, 40, 50]")

    await node.submit_task(task)

    # =========================================================
    # WAIT FOR RESULT
    # =========================================================

    print()
    print("[SUBMITTER] Waiting for task result...")

    result = await node.wait_for_task(
        task.task_id,
        timeout=30,
    )

    # =========================================================
    # FINAL TASK STATE
    # =========================================================

    print()
    print("=" * 60)
    print("FINAL TASK STATE")
    print("=" * 60)

    if result:

        status = result.status

        if hasattr(status, "value"):
            status = status.value

        print(
            f"Task ID : {result.task_id}"
        )

        print(
            f"Status  : {status}"
        )

        print(
            f"Worker  : {result.assigned_peer}"
        )

        print(
            f"Result  : {result.result}"
        )

        print(
            f"Error   : {result.error}"
        )

    else:

        print("No task result received.")

    # =========================================================
    # SHUTDOWN
    # =========================================================

    print()
    print("[SUBMITTER] Shutting down...")

    await node.stop()

    print("[SUBMITTER] Stopped successfully.")


if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        print()
        print("Submitter stopped.")