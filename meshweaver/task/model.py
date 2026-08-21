from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable
import uuid


class TaskStatus(Enum):

    PENDING = "PENDING"
    ASSIGNED = "ASSIGNED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass
class Task:

    task_id: str = field(
        default_factory=lambda: str(uuid.uuid4())
    )

    function_name: str = ""

    function: Callable | None = None

    args: tuple = ()

    kwargs: dict = field(
        default_factory=dict
    )

    status: TaskStatus = TaskStatus.PENDING

    assigned_peer: str | None = None

    result: Any = None

    error: str | None = None

    def assign(
        self,
        peer_id: str,
    ):

        self.assigned_peer = peer_id
        self.status = TaskStatus.ASSIGNED

    def start(self):

        self.status = TaskStatus.RUNNING

    def complete(
        self,
        result,
    ):

        self.result = result
        self.status = TaskStatus.COMPLETED

    def fail(
        self,
        error,
    ):

        self.error = str(error)
        self.status = TaskStatus.FAILED