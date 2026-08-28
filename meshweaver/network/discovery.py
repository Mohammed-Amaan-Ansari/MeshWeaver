import json


HELLO = "HELLO"

WELCOME = "WELCOME"

TASK = "TASK"

RESULT = "RESULT"

GOSSIP = "GOSSIP"

HEARTBEAT = "HEARTBEAT"

FIND_NODE = "FIND_NODE"
FIND_NODE_RESPONSE = "FIND_NODE_RESPONSE"

def create_find_node(
    node_id,
    target_id,
):
    """
    Create a Kademlia FIND_NODE request.
    """

    return {
        "type": FIND_NODE,
        "node_id": node_id,
        "target_id": target_id.hex(),
    }


def create_find_node_response(
    node_id,
    peers,
):
    """
    Create a response containing
    the closest known peers.
    """

    return {
        "type": FIND_NODE_RESPONSE,
        "node_id": node_id,
        "peers": peers,
    }
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


def create_heartbeat(
    node_id,
):

    return {
        "type": HEARTBEAT,
        "node_id": node_id,
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