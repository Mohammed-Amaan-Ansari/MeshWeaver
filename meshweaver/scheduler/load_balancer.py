def calculate_load_score(
    load,
):

    cpu = load.get(
        "cpu",
        100,
    )

    memory = load.get(
        "memory",
        100,
    )

    return (
        cpu + memory
    ) / 2


def select_best_peer(
    peer_loads,
):

    if not peer_loads:
        return None

    return min(
        peer_loads,
        key=lambda peer_id:
            calculate_load_score(
                peer_loads[peer_id]
            )
    )