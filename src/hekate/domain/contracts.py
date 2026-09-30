from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256


MAX_CONTRACT_BYTES = 1_048_576
MAX_CONTRACT_ITEMS = 1_000


class DuplicateJsonKeyError(ValueError):
    pass


def check_json_payload(payload: bytes) -> bytes:
    if len(payload) > MAX_CONTRACT_BYTES:
        raise ValueError(f"JSON contract exceeds {MAX_CONTRACT_BYTES} bytes")

    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise DuplicateJsonKeyError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        json.loads(payload.decode("utf-8"), object_pairs_hook=reject_duplicates)
    except DuplicateJsonKeyError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as error:
        raise ValueError("invalid UTF-8 JSON contract") from error
    return payload


def canonical_json(value: object) -> str:
    def normalize(item: object) -> object:
        if isinstance(item, Decimal):
            if not item.is_finite():
                raise ValueError("non-finite Decimal is not valid JSON")
            return str(item)
        if isinstance(item, datetime):
            if item.tzinfo is None or item.utcoffset() is None:
                raise ValueError("timestamps must be timezone-aware")
            return item.isoformat()
        if isinstance(item, StrEnum):
            return item.value
        if is_dataclass(item):
            return normalize(asdict(item))
        if hasattr(item, "model_dump"):
            return normalize(item.model_dump(mode="json", by_alias=True))
        if isinstance(item, float):
            raise TypeError("float is not allowed in canonical business payloads")
        if isinstance(item, dict):
            result: dict[str, object] = {}
            for key, child in item.items():
                if not isinstance(key, str):
                    raise TypeError("JSON object keys must be strings")
                result[key] = normalize(child)
            return result
        if isinstance(item, (list, tuple)):
            return [normalize(child) for child in item]
        if item is None or isinstance(item, (str, int, bool)):
            return item
        raise TypeError(f"unsupported canonical JSON value: {type(item).__name__}")

    return json.dumps(normalize(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def canonical_json_hash(value: object) -> str:
    return sha256(canonical_json(value).encode("utf-8")).hexdigest()
