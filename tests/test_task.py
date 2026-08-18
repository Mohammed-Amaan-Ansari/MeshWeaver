from meshweaver.task.model import (
    Task,
    TaskStatus,
)


def test_task_creation():

    task = Task(
        function_name="add",
        args=(10, 20),
    )

    assert task.status == TaskStatus.PENDING
    assert task.function_name == "add"
    assert task.args == (10, 20)


def test_task_lifecycle():

    task = Task(
        function_name="add",
        args=(10, 20),
    )

    assert task.status == TaskStatus.PENDING

    task.assign("NODE_B")

    assert task.status == TaskStatus.ASSIGNED
    assert task.assigned_peer == "NODE_B"

    task.start()

    assert task.status == TaskStatus.RUNNING

    task.complete(30)

    assert task.status == TaskStatus.COMPLETED
    assert task.result == 30


def test_task_failure():

    task = Task(
        function_name="divide",
        args=(10, 0),
    )

    task.assign("NODE_B")
    task.start()
    task.fail("Division by zero")

    assert task.status == TaskStatus.FAILED
    assert task.error == "Division by zero"