import json


# =========================================================
# MESSAGE TYPES
# =========================================================

HELLO = "HELLO"
WELCOME = "WELCOME"

GOSSIP = "GOSSIP"

HEARTBEAT = "HEARTBEAT"
HEARTBEAT_ACK = "HEARTBEAT_ACK"

TASK = "TASK"
RESULT = "RESULT"

FIND_NODE = "FIND_NODE"
FIND_NODE_RESPONSE = "FIND_NODE_RESPONSE"

STORE = "STORE"
STORE_RESPONSE = "STORE_RESPONSE"

FIND_VALUE = "FIND_VALUE"
FIND_VALUE_RESPONSE = "FIND_VALUE_RESPONSE"


# =========================================================
# DISCOVERY
# =========================================================

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


# =========================================================
# HEARTBEAT
# =========================================================

def create_heartbeat(node_id):

    return {
        "type": HEARTBEAT,
        "node_id": node_id,
    }


def create_heartbeat_ack(node_id):

    return {
        "type": HEARTBEAT_ACK,
        "node_id": node_id,
    }


# =========================================================
# GOSSIP
# =========================================================

def create_gossip(
    node_id,
    load,
):

    return {
        "type": GOSSIP,
        "node_id": node_id,
        "load": load,
    }


# =========================================================
# TASK
# =========================================================

def create_task_message(
    sender_id,
    task_id,
    task_data,
):

    return {
        "type": TASK,
        "sender_id": sender_id,
        "task_id": task_id,
        "task_data": task_data.hex(),
    }


# =========================================================
# RESULT
# =========================================================

def create_result_message(
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


# =========================================================
# DHT FIND NODE
# =========================================================

def create_find_node(
    node_id,
    target_id,
):

    return {
        "type": FIND_NODE,
        "node_id": node_id,
        "target_id": target_id.hex(),
    }


def create_find_node_response(
    node_id,
    peers,
):

    return {
        "type": FIND_NODE_RESPONSE,
        "node_id": node_id,
        "peers": peers,
    }


# =========================================================
# DHT STORE
# =========================================================

def create_store(
    node_id,
    key,
    value,
):

    return {
        "type": STORE,
        "node_id": node_id,
        "key": key,
        "value": value,
    }


def create_store_response(
    node_id,
    key,
    success,
):

    return {
        "type": STORE_RESPONSE,
        "node_id": node_id,
        "key": key,
        "success": success,
    }


# =========================================================
# DHT FIND VALUE
# =========================================================

def create_find_value(
    node_id,
    key,
):

    return {
        "type": FIND_VALUE,
        "node_id": node_id,
        "key": key,
    }


def create_find_value_response(
    node_id,
    key,
    value=None,
    found=False,
    peers=None,
):

    return {
        "type": FIND_VALUE_RESPONSE,
        "node_id": node_id,
        "key": key,
        "value": value,
        "found": found,
        "peers": peers or [],
    }


# =========================================================
# SERIALIZATION
# =========================================================

def encode_message(message):

    return json.dumps(
        message,
        separators=(",", ":"),
    ).encode("utf-8")


def decode_message(data):

    return json.loads(
        data.decode("utf-8")
    )