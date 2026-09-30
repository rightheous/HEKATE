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
    UNEXPECTED = "Unexpected"


def classify_failure(error: Exception) -> FailureClass:
    if isinstance(error, Conflict):
        return FailureClass.CONFLICT
    if isinstance(error, StaleInput):
        return FailureClass.STALE_INPUT
    if isinstance(error, PolicyDenied):
        return FailureClass.POLICY_DENIED
    if isinstance(error, BudgetDenied):
        return FailureClass.BUDGET_DENIED
    if isinstance(error, UnknownExecution):
        return FailureClass.UNKNOWN_EXECUTION
    if isinstance(error, StorageUnavailable):
        return FailureClass.STORAGE_UNAVAILABLE
    if isinstance(error, ProviderTransient):
        return FailureClass.PROVIDER_TRANSIENT
    return FailureClass.UNEXPECTED
