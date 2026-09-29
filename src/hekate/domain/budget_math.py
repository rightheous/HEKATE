from __future__ import annotations

from .models import (
    AccountSnapshot, CostAssessment, NormalizedUsage, PriceTable, RuntimeLimits,
)
from .types import Money


def estimate_envelope_cost(limits: RuntimeLimits, rates: PriceTable) -> Money:
    raise NotImplementedError


def price_usage(usage: NormalizedUsage, rates: PriceTable) -> CostAssessment:
    raise NotImplementedError


def available_budget(account: AccountSnapshot) -> Money:
    raise NotImplementedError
