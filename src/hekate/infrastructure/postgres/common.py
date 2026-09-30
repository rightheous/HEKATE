from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from hekate.domain.contracts import canonical_json


def new_key() -> str:
    return str(uuid4())


def json_value(value: object) -> object:
    return json.loads(canonical_json(value))


def aware_now() -> datetime:
    return datetime.now(UTC)


def require_money(value: Decimal) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
        raise ValueError("money must be a finite nonnegative Decimal")
    return value
