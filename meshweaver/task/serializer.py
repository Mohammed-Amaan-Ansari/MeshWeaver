import cloudpickle

from meshweaver.task.model import Task


def serialize_task(
    task: Task,
) -> bytes:

    if not isinstance(
        task,
        Task,
    ):

        raise TypeError(
            "serialize_task() expects "
            "a Task object"
        )

    return cloudpickle.dumps(
        task
    )


def deserialize_task(
    data: bytes,
) -> Task:

    if not isinstance(
        data,
        bytes,
    ):

        raise TypeError(
            "deserialize_task() expects "
            "bytes"
        )

    task = cloudpickle.loads(
        data
    )

    if not isinstance(
        task,
        Task,
    ):

        raise TypeError(
            "Deserialized object is "
            "not a Task"
        )

    return task