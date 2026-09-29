from __future__ import annotations

from hekate.domain.models import Attempt, AuthorizationSnapshot, Task
from hekate.domain.types import AttemptId, ScopeId, TaskId


class PostgresTaskRepository:
    def __init__(self, connection: object) -> None:
        self.connection = connection

    async def lock_scope(self, scope: ScopeId) -> AuthorizationSnapshot: raise NotImplementedError
    async def insert_task(self, task: Task) -> None: raise NotImplementedError
    async def lock_task(self, task_id: TaskId) -> Task: raise NotImplementedError
    async def get_attempt(self, attempt_id: AttemptId, for_update: bool = False) -> Attempt: raise NotImplementedError
    async def insert_attempt(self, attempt: Attempt) -> None: raise NotImplementedError
    async def compare_and_set_state(self, task_id: TaskId, expected: object, update: object) -> bool: raise NotImplementedError
    async def increment_counter_if_below(self, task_id: TaskId, counter: str, cap: int) -> bool: raise NotImplementedError
