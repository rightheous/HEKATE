from __future__ import annotations

import argparse
from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="hekate")
    parser.add_argument("command", choices=("serve", "worker", "reconcile", "doctor"))
    parser.parse_args(argv)
    raise NotImplementedError("CLI wiring is provided by bootstrap during implementation")


if __name__ == "__main__":
    raise SystemExit(main())
