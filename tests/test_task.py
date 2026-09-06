from meshweaver.task.model import (
    Task,
    TaskStatus,
)

from meshweaver.task.executor import (
    execute_task,
)

from meshweaver.task.serializer import (
    serialize_task,
    deserialize_task,
)


def add(a, b):

    return a + b


def test_task_execution():

    task = Task(
        function=add,
        args=(10, 20),
    )

    execute_task(task)

    assert (
        task.status
        == TaskStatus.COMPLETED
    )

    assert task.result == 30


def test_task_failure():

    def broken():
        raise ValueError(
            "Something went wrong"
        )

    task = Task(
        function=broken
    )

    execute_task(task)

    assert (
        task.status
        == TaskStatus.FAILED
    )


def test_task_serialization():

    task = Task(
        function=add,
        args=(5, 7),
    )

    data = serialize_task(
        task
    )

    restored = deserialize_task(
        data
    )

    assert isinstance(
        restored,
        Task,
    )

    assert restored.args == (
        5,
        7,
    )

    execute_task(
        restored
    )

    assert restored.result == 12