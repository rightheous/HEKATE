from __future__ import annotations

from enum import StrEnum


class HekateError(Exception):
    """Base class for errors that can be mapped to a durable failure class."""


class Conflict(HekateError):
    pass


class StaleInput(HekateError):
    pass


class PolicyDenied(HekateError):
    pass


class BudgetDenied(HekateError):
    pass


class UnknownExecution(HekateError):
    pass


class StorageUnavailable(HekateError):
    pass


class ProviderTransient(HekateError):
    pass


class FailureClass(StrEnum):
    CONFLICT = "Conflict"
    STALE_INPUT = "StaleInput"
    POLICY_DENIED = "PolicyDenied"
    BUDGET_DENIED = "BudgetDenied"
    UNKNOWN_EXECUTION = "UnknownExecution"
    STORAGE_UNAVAILABLE = "StorageUnavailable"
    PROVIDER_TRANSIENT = "ProviderTransient"


def classify_failure(error: Exception) -> FailureClass:
    raise NotImplementedError
