from meshweaver.task.model import (
    Task,
    TaskStatus,
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

        # Mark task as running
        task.start()

        # Execute function
        result = task.function(
            *task.args,
            **task.kwargs,
        )

        # Store result
        task.complete(
            result
        )

    except Exception as exc:

        task.fail(
            str(exc)
        )

    return task