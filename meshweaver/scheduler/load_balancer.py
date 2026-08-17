def calculate_load_score(load):
    """
    Calculate a simple workload score.

    Lower score = better worker.

    CPU and RAM are equally weighted for now.
    """

    cpu = load.get("cpu", 100)
    memory = load.get("memory", 100)

    return (cpu + memory) / 2


def select_best_peer(peer_loads):
    """
    Select the peer with the lowest load score.

    peer_loads format:

    {
        "NODE_A": {
            "cpu": 20,
            "memory": 40
        },
        "NODE_B": {
            "cpu": 60,
            "memory": 30
        }
    }
    """

    if not peer_loads:
        return None

    return min(
        peer_loads,
        key=lambda peer_id:
            calculate_load_score(peer_loads[peer_id])
    )