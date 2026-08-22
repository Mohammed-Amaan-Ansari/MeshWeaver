import json


HELLO = "HELLO"
WELCOME = "WELCOME"
TASK = "TASK"
RESULT = "RESULT"


def create_hello(
    node_id,
    port,
):

    return {
        "type": HELLO,
        "node_id": node_id,
        "port": port,
    }


def create_welcome(
    node_id,
    port,
):

    return {
        "type": WELCOME,
        "node_id": node_id,
        "port": port,
    }


def create_result(
    sender_id,
    task_id,
    status,
    result=None,
    error=None,
):

    return {
        "type": RESULT,
        "sender_id": sender_id,
        "task_id": task_id,
        "status": status,
        "result": result,
        "error": error,
    }


def encode_message(
    message,
):

    return json.dumps(
        message
    ).encode("utf-8")


def decode_message(
    data,
):

    return json.loads(
        data.decode("utf-8")
    )