from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path


SOURCE_COMMIT = "0f3a71be15af836d277c9f918adfafb45732677e"
SOURCE_SHA256 = "95170cd1c105a5b41a1b2dce73b0fae8ce8011ef7897600828bb2babe8b26e5d"
MAX_CODEPOINTS = 0x110000


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _block(text: str, name: str, end: str) -> str:
    start = text.index(name)
    return text[start:text.index(end, start)]


def build(source_path: Path) -> dict[str, object]:
    raw = source_path.read_bytes()
    if sha256(raw) != SOURCE_SHA256:
        raise ValueError("Unicode reference source digest does not match llama.cpp b10760")
    text = raw.decode("utf-8")
    flags_block = _block(text, "unicode_ranges_flags =", "const std::unordered_set<uint32_t> unicode_set_whitespace")
    starts = [(int(a, 16), int(b, 16)) for a, b in re.findall(r"\{0x([0-9A-Fa-f]+), 0x([0-9A-Fa-f]+)\}", flags_block)]
    if not starts or starts[0][0] != 0 or starts[-1][0] != MAX_CODEPOINTS:
        raise ValueError("llama.cpp Unicode flag ranges are incomplete")

    whitespace_block = _block(text, "unicode_set_whitespace =", "const std::initializer_list<std::pair<uint32_t, uint32_t>> unicode_map_lowercase")
    whitespace = {int(value, 16) for value in re.findall(r"0x([0-9A-Fa-f]+)", whitespace_block)}
    # C++ initializes missing code points as UNDEFINED (nonzero), so the
    # tokenizer's "flags.as_uint()" fallback must remain truthy for them.
    flags = bytearray([16]) * MAX_CODEPOINTS
    for (start, source_flags), (end, _) in zip(starts, starts[1:]):
        compact = (1 if source_flags & 0x0004 else 0) | (2 if source_flags & 0x0010 else 0) | (4 if source_flags & 0x0002 else 0)
        if source_flags:
            compact |= 16
        if compact:
            flags[start:end] = bytes([compact]) * (end - start)
    for codepoint in whitespace:
        flags[codepoint] |= 8

    ranges: list[list[int]] = []
    start = 0
    current = flags[0]
    for codepoint in range(1, MAX_CODEPOINTS + 1):
        next_flag = flags[codepoint] if codepoint < MAX_CODEPOINTS else -1
        if next_flag != current:
            if current:
                ranges.append([start, codepoint, current])
            start, current = codepoint, next_flag
    result = {
        "source": {
            "repository": "ggml-org/llama.cpp",
            "commit": SOURCE_COMMIT,
            "file": "src/unicode-data.cpp",
            "sha256": SOURCE_SHA256,
            "license": "MIT",
            "flags": {"letter": 1, "mark": 2, "number": 4, "whitespace": 8, "nonzero": 16},
        },
        "ranges": ranges,
    }
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Derive the minimal Qwen35 Unicode tokenizer property table.")
    parser.add_argument("--source", type=Path, required=True, help="src/unicode-data.cpp from the pinned b10760 checkout")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = build(args.source)
    payload = json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    args.output.write_text(payload, encoding="utf-8")
    print(json.dumps({"path": str(args.output), "sha256": sha256(payload.encode()), "ranges": len(result["ranges"])}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
