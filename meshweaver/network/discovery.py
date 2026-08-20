import json


HELLO = "HELLO"
WELCOME = "WELCOME"
TASK = "TASK"


def create_hello(node_id, port):

    return {
        "type": HELLO,
        "node_id": node_id,
        "port": port,
    }


def create_welcome(node_id, port):

    return {
        "type": WELCOME,
        "node_id": node_id,
        "port": port,
    }


def encode_message(message):

    return json.dumps(
        message
    ).encode("utf-8")


def decode_message(data):

    return json.loads(
        data.decode("utf-8")
    )