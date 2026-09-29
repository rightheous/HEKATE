from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str
    bridge_socket: Path
    config_dir: Path
    policy: Mapping[str, object]
    models: Mapping[str, object]
    pricing: Mapping[str, object]


def load_settings(env: Mapping[str, str], config_dir: Path) -> Settings:
    raise NotImplementedError


def validate_settings(settings: Settings) -> None:
    raise NotImplementedError
