from __future__ import annotations

from dataclasses import replace

from hekate.domain.models import PriceTable, ProviderExecutionProfile, QwenOllamaCandidateProfile
from hekate.infrastructure.letta.qwen_ollama import qwen35_native_json_schema_test_execution_profile


def qwen35_native_json_schema_local_execution_profile(
    candidate: QwenOllamaCandidateProfile | None = None,
) -> tuple[ProviderExecutionProfile, PriceTable]:
    """Keep the frozen candidate identity while classifying its local tariff accurately."""
    profile, synthetic_prices = qwen35_native_json_schema_test_execution_profile(candidate)
    return profile, replace(synthetic_prices, synthetic=False)


def validate_qwen35_native_json_schema_local_profile(
    profile: ProviderExecutionProfile,
    price_table: PriceTable,
) -> None:
    expected_profile, expected_prices = qwen35_native_json_schema_local_execution_profile()
    if (
        profile.content_digest != expected_profile.content_digest
        or price_table != expected_prices
        or profile.test_only is not True
        or profile.provider != "ollama-local"
        or price_table.synthetic is not False
    ):
        raise ValueError("local Qwen profile or external-tariff classification changed")
