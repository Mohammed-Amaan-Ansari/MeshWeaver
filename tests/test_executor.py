from meshweaver.task.model import (
    Task,
    TaskStatus,
)

from meshweaver.task.executor import (
    execute_task,
)


def add(
    a,
    b,
):

    return a + b


def test_task_execution():

    task = Task(
        function_name="add",
        function=add,
        args=(10, 20),
    )

    result = execute_task(
        task
    )

    assert (
        result.status
        == TaskStatus.COMPLETED
    )

    assert result.result == 30


def test_task_execution_with_failure():

    def divide(
        a,
        b,
    ):

        return a / b

    task = Task(
        function_name="divide",
        function=divide,
        args=(10, 0),
    )

    result = execute_task(
        task
    )

    assert (
        result.status
        == TaskStatus.FAILED
    )

    assert result.error is not None