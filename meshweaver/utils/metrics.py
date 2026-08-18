import psutil


def get_system_load():

    return {
        "cpu": psutil.cpu_percent(
            interval=None
        ),
        "memory": psutil.virtual_memory().percent,
    }