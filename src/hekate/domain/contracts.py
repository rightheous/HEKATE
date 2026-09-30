from __future__ import annotations

import json


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
