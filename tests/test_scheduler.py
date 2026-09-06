from meshweaver.scheduler.load_balancer import (
    calculate_load_score,
    select_best_peer,
)


def test_load_score():

    score = calculate_load_score(
        {
            "cpu": 20,
            "memory": 40,
        }
    )

    assert score == 30


def test_best_peer():

    peer_loads = {

        "NODE_A": {
            "cpu": 70,
            "memory": 50,
        },

        "NODE_B": {
            "cpu": 20,
            "memory": 30,
        },

        "NODE_C": {
            "cpu": 40,
            "memory": 40,
        },
    }

    assert (
        select_best_peer(
            peer_loads
        )
        == "NODE_B"
    )


def test_empty_loads():

    assert (
        select_best_peer({})
        is None
    )