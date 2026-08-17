from meshweaver.scheduler.load_balancer import (
    calculate_load_score,
    select_best_peer,
)


def test_calculate_load_score():

    load = {
        "cpu": 20,
        "memory": 40,
    }

    score = calculate_load_score(load)

    assert score == 30


def test_select_best_peer():

    peer_loads = {
        "NODE_A": {
            "cpu": 20,
            "memory": 40,
        },

        "NODE_B": {
            "cpu": 70,
            "memory": 80,
        },

        "NODE_C": {
            "cpu": 10,
            "memory": 20,
        },
    }

    best_peer = select_best_peer(peer_loads)

    assert best_peer == "NODE_C"