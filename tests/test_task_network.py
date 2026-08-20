from meshweaver.task.model import Task

from meshweaver.task.network import (
    create_task_message,
    extract_task,
)


def test_task_network_round_trip():

    task = Task(
        function_name="add",
        args=(10, 20),
    )

    message = create_task_message(
        "NODE_A",
        task,
    )

    assert message["type"] == "TASK"

    assert (
        message["sender_id"]
        == "NODE_A"
    )

    restored_task = extract_task(
        message
    )

    assert (
        restored_task.task_id
        == task.task_id
    )

    assert (
        restored_task.function_name
        == "add"
    )

    assert (
        restored_task.args
        == (10, 20)
    )