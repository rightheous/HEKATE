from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ExportReport:
    output_dir: Path
    schemas: tuple[str, ...]


def write_schemas(output_dir: Path) -> ExportReport:
    raise NotImplementedError


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=Path("contracts/generated"))
    parser.parse_args()
    raise NotImplementedError


if __name__ == "__main__":
    raise SystemExit(main())
