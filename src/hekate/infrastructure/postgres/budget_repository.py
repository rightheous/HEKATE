from __future__ import annotations

from collections.abc import Sequence

from hekate.domain.models import Account, BudgetReservation, Page, Reservation, UsageRecord


class PostgresBudgetRepository:
    def __init__(self, connection: object) -> None:
        self.connection = connection

    async def lock_accounts(self, account_ids: Sequence[str]) -> Sequence[Account]: raise NotImplementedError
    async def insert_reservation(self, reservation: BudgetReservation) -> None: raise NotImplementedError
    async def allocate_call(self, permit: object) -> None: raise NotImplementedError
    async def upsert_usage(self, usage: UsageRecord) -> bool: raise NotImplementedError
    async def apply_settlement(self, settlement: object) -> None: raise NotImplementedError
    async def list_pending(self, cursor: object) -> Page[Reservation]: raise NotImplementedError
