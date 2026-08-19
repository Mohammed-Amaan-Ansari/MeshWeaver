from meshweaver.task.model import (
    Task,
    TaskStatus,
)

from meshweaver.task.serializer import (
    serialize_task,
    deserialize_task,
)


def test_task_serialization():

    task = Task(
        function_name="add",
        args=(10, 20),
    )

    data = serialize_task(task)

    assert isinstance(data, bytes)
    assert len(data) > 0


def test_task_deserialization():

    task = Task(
        function_name="add",
        args=(10, 20),
    )

    data = serialize_task(task)

    restored_task = deserialize_task(data)

    assert isinstance(restored_task, Task)

    assert restored_task.task_id == task.task_id

    assert (
        restored_task.function_name
        == task.function_name
    )

    assert restored_task.args == task.args

    assert (
        restored_task.status
        == TaskStatus.PENDING
    )


def test_task_round_trip():

    task = Task(
        function_name="multiply",
        args=(5, 6),
        kwargs={
            "round_result": True
        },
    )

    task.assign("NODE_B")
    task.start()

    data = serialize_task(task)

    restored_task = deserialize_task(data)

    assert (
        restored_task.task_id
        == task.task_id
    )

    assert (
        restored_task.function_name
        == "multiply"
    )

    assert restored_task.args == (5, 6)

    assert restored_task.kwargs == {
        "round_result": True
    }

    assert (
        restored_task.status
        == TaskStatus.RUNNING
    )

    assert (
        restored_task.assigned_peer
        == "NODE_B"
    )