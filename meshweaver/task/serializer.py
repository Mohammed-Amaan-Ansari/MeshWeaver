import cloudpickle

from meshweaver.task.model import Task


def serialize_task(task: Task) -> bytes:
    """
    Convert a Task object into bytes.
    """

    if not isinstance(task, Task):
        raise TypeError(
            "serialize_task() expects a Task object"
        )

    return cloudpickle.dumps(task)

