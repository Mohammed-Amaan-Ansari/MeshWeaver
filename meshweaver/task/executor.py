from meshweaver.task.model import (
    Task,
)


def execute_task(
    task: Task,
) -> Task:

    if not isinstance(
        task,
        Task,
    ):
        raise TypeError(
            "execute_task() expects "
            "a Task object"
        )

    if task.function is None:

        task.fail(
            "Task does not contain "
            "a callable function."
        )

        return task

    try:

        task.start()

        result = task.function(
            *task.args,
            **task.kwargs,
        )

        task.complete(
            result
        )

    except Exception as exc:

        task.fail(
            str(exc)
        )

    return task