from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ExportReport:
    output_dir: Path
    schemas: tuple[str, ...]


def write_schemas(output_dir: Path) -> ExportReport:
    from hekate.domain.capsules import export_schemas

    schemas = export_schemas()
    output_dir.mkdir(parents=True, exist_ok=True)
    for filename, schema in schemas.items():
        content = json.dumps(schema, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        (output_dir / filename).write_text(content, encoding="utf-8")
    return ExportReport(output_dir=output_dir, schemas=tuple(sorted(schemas)))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=Path("contracts/generated"))
    args = parser.parse_args()
    report = write_schemas(args.output_dir)
    for filename in report.schemas:
        print(report.output_dir / filename)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
