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
        port=9001,
        node_id="SUBMITTER",

        bootstrap_peers=[
            ("127.0.0.1", 9002),
             
        ],

        # Use the same security key as the worker nodes.
        security_key=config.key,
    )

    # =========================================================
    # START NODE COMPONENTS
    # =========================================================

    print()
    print("=" * 60)
    print("Starting MeshWeaver Submitter")
    print("=" * 60)

    print("[SUBMITTER] Security configuration: ENABLED")

    # Start networking without blocking this example forever.
    await node.start_components()

    # Give the submitter time to discover and authenticate
    # with the worker nodes.
    await asyncio.sleep(7)

    # =========================================================
    # DISPLAY NETWORK STATE
    # =========================================================

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

    print()
    print("=" * 60)
    print("SUBMITTING TASK")
    print("=" * 60)

    # =========================================================
    # SUBMIT TASK
    # =========================================================

    await node.submit_task(task)

    # Wait for the task result.
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

    else:

        print("No task result received.")

    # =========================================================
    # SHUTDOWN
    # =========================================================

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
