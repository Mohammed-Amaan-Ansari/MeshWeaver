from meshweaver.task.model import Task

from meshweaver.task.serializer import (
    serialize_task,
    deserialize_task,
)


def create_task_message(
    sender_id: str,
    task: Task,
):

    task_data = serialize_task(
        task
    )

    return {
        "type": "TASK",
        "sender_id": sender_id,
        "task_data": task_data.hex(),
    }


def extract_task(
    message,
) -> Task:

    task_data = bytes.fromhex(
        message["task_data"]
    )

    return deserialize_task(
        task_data
    )