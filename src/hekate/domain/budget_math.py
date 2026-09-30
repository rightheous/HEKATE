from __future__ import annotations

from decimal import Decimal

from hekate.domain.errors import BudgetDenied
from .models import (
    AccountSnapshot, CostAssessment, NormalizedUsage, PriceTable, RuntimeLimits,
)
from .types import Money


def estimate_envelope_cost(limits: RuntimeLimits, rates: PriceTable) -> Money:
    if limits.max_input_tokens < 0 or limits.max_output_tokens < 0:
        raise ValueError("token limits must be nonnegative")
    if not rates.input_usd_per_million.is_finite() or not rates.output_usd_per_million.is_finite():
        raise ValueError("prices must be finite")
    return (
        Decimal(limits.max_input_tokens) * rates.input_usd_per_million
        + Decimal(limits.max_output_tokens) * rates.output_usd_per_million
    ) / Decimal(1_000_000)


def price_usage(usage: NormalizedUsage, rates: PriceTable) -> CostAssessment:
    if usage.reported_cost_usd is not None:
        if not usage.reported_cost_usd.is_finite() or usage.reported_cost_usd < 0:
            raise ValueError("reported usage cost must be finite and nonnegative")
        return CostAssessment(amount=usage.reported_cost_usd)
    if usage.completeness != "COMPLETE" or usage.input_tokens is None or usage.output_tokens is None:
        raise ValueError("usage is incomplete")
    return CostAssessment(
        amount=(
            Decimal(usage.input_tokens) * rates.input_usd_per_million
            + Decimal(usage.output_tokens) * rates.output_usd_per_million
        ) / Decimal(1_000_000),
        estimated=True,
        details={"pricing_version": rates.version, "model": rates.model},
    )


def available_budget(account: AccountSnapshot) -> Money:
    for amount in (account.limit_amount, account.spent_amount, account.held_amount):
        if not amount.is_finite() or amount < 0:
            raise ValueError("budget account values must be finite and nonnegative")
    return account.available
