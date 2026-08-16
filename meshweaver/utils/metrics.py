import psutil


def get_system_load():
    """
    Return the current CPU and memory usage
    of this node.
    """

    return {
        "cpu": psutil.cpu_percent(interval=None),
        "memory": psutil.virtual_memory().percent,
    }