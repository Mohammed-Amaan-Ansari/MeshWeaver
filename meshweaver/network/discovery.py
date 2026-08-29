import json


HELLO = "HELLO"

WELCOME = "WELCOME"

TASK = "TASK"

RESULT = "RESULT"

GOSSIP = "GOSSIP"

HEARTBEAT = "HEARTBEAT"

FIND_NODE = "FIND_NODE"
FIND_NODE_RESPONSE = "FIND_NODE_RESPONSE"

STORE = "STORE"
FIND_VALUE = "FIND_VALUE"

STORE_RESPONSE = "STORE_RESPONSE"
FIND_VALUE_RESPONSE = "FIND_VALUE_RESPONSE"

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

def create_store(
    node_id,
    key,
    value,
):
    """
    Create a DHT STORE request.
    """

    return {
        "type": STORE,
        "node_id": node_id,
        "key": key,
        "value": value,
    }


def create_find_value(
    node_id,
    key,
):
    """
    Create a DHT FIND_VALUE request.
    """

    return {
        "type": FIND_VALUE,
        "node_id": node_id,
        "key": key,
    }


def create_store_response(
    node_id,
    key,
    success,
):
    """
    Create STORE response.
    """

    return {
        "type": STORE_RESPONSE,
        "node_id": node_id,
        "key": key,
        "success": success,
    }


def create_find_value_response(
    node_id,
    key,
    value,
    found,
):
    """
    Create FIND_VALUE response.
    """

    return {
        "type": FIND_VALUE_RESPONSE,
        "node_id": node_id,
        "key": key,
        "value": value,
        "found": found,
    }