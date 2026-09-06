from meshweaver.task.serializer import serialize_task, deserialize_task


def create_task_message(sender_id, task):
    """
    Create a network message containing a serialized Task.

    The task is serialized with cloudpickle and converted to
    hexadecimal so it can safely travel inside a JSON message.
    """

    serialized_task = serialize_task(task)

    return {
        "type": "TASK",
        "sender_id": sender_id,
        "task_id": task.task_id,
        "task_data": serialized_task.hex(),
    }


def extract_task(message):
    """
    Extract and deserialize a Task from a TASK network message.
    """

    if message.get("type") != "TASK":
        raise ValueError("Invalid message type. Expected TASK.")

    task_data = message.get("task_data")

    if not task_data:
        raise ValueError("TASK message does not contain task_data.")

    try:
        serialized_task = bytes.fromhex(task_data)
    except ValueError as exc:
        raise ValueError("Invalid hexadecimal task_data.") from exc

    return deserialize_task(serialized_task)