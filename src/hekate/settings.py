from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from urllib.parse import urlsplit

import yaml


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str
    node_bin: str
    bridge_entry: Path
    letta_url: str
    letta_token: str | None
    worker_id: str
    runtime_mode: str
    config_dir: Path
    policy: Mapping[str, object]
    models: Mapping[str, object]
    pricing: Mapping[str, object]


def _yaml(path: Path) -> Mapping[str, object]:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"configuration must be a YAML object: {path.name}")
    return value


def load_settings(env: Mapping[str, str], config_dir: Path) -> Settings:
    root = config_dir.parent
    return Settings(
        database_url=env.get("HEKATE_DATABASE_URL", ""),
        node_bin=env.get("HEKATE_NODE_BIN", "node"),
        bridge_entry=Path(env.get("HEKATE_BRIDGE_ENTRY", root / "bridge/letta/dist/main.js")),
        letta_url=env.get("HEKATE_LETTA_URL", ""),
        letta_token=env.get("HEKATE_LETTA_TOKEN") or None,
        worker_id=env.get("HEKATE_WORKER_ID", ""),
        runtime_mode=env.get("HEKATE_RUNTIME_MODE", "production"),
        config_dir=config_dir,
        policy=_yaml(config_dir / "policy.yaml"),
        models=_yaml(config_dir / "models.yaml"),
        pricing=_yaml(config_dir / "pricing.yaml"),
    )


def validate_settings(settings: Settings) -> None:
    if not settings.database_url.startswith("postgresql+psycopg://"):
        raise ValueError("HEKATE requires PostgreSQL via psycopg")
    if settings.runtime_mode not in {"production", "test"}:
        raise ValueError("HEKATE_RUNTIME_MODE must be production or test")
    if not settings.worker_id or len(settings.worker_id) > 128:
        raise ValueError("HEKATE_WORKER_ID must be a stable unique worker identity")
    if urlsplit(settings.letta_url).scheme not in {"ws", "wss"} or not urlsplit(settings.letta_url).hostname:
        raise ValueError("HEKATE_LETTA_URL must be a configured WebSocket URL")
    node = shutil.which(settings.node_bin) or settings.node_bin
    if not settings.bridge_entry.is_file():
        raise ValueError("built private JSONL bridge entry is missing")
    version = subprocess.run([node, "--version"], capture_output=True, text=True, check=True, timeout=10).stdout.strip()
    if version != "v22.19.0":
        raise ValueError("HEKATE bridge requires the pinned Node.js 22.19.0 runtime")
    if settings.runtime_mode == "production":
        model_profiles = [settings.models.get("hekate"), settings.models.get("critic")]
        has_verified_model = any(
            isinstance(profile, dict)
            and profile.get("verified") is True
            and profile.get("tokenizer_verified") is True
            for profile in model_profiles
        )
        pricing_verified = settings.pricing.get("version") not in {None, "unconfigured"} and bool(settings.pricing.get("prices"))
        if not has_verified_model or not pricing_verified:
            raise ValueError("no approved model, tokenizer, and pricing profile; production dispatch stays closed")
