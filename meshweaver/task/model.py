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
        default_factory=lambda:
            str(uuid.uuid4())
    )

    function_name: str = ""

    function: Callable | None = None

    args: tuple = ()

    kwargs: dict = field(
        default_factory=dict
    )

    status: TaskStatus = (
        TaskStatus.PENDING
    )

    assigned_peer: str | None = None

    result: Any = None

    error: str | None = None

    attempts: int = 0

    max_attempts: int = 3

    def assign(
        self,
        peer_id,
    ):

        self.assigned_peer = peer_id

        self.status = (
            TaskStatus.ASSIGNED
        )

        self.attempts += 1

    def start(self):

        self.status = (
            TaskStatus.RUNNING
        )

    def complete(
        self,
        result,
    ):

        self.result = result

        self.status = (
            TaskStatus.COMPLETED
        )

    def fail(
        self,
        error,
    ):

        self.error = str(error)

        self.status = (
            TaskStatus.FAILED
        )

    def can_retry(self):

        return (
            self.attempts
            < self.max_attempts
        )