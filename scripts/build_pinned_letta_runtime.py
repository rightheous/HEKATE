#!/usr/bin/env python3
"""Build the App Server image from versions.lock.json and its pinned runtime patch."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import integration_probe  # noqa: E402


def main() -> int:
    lock = json.loads((ROOT / "integration/letta/versions.lock.json").read_text(encoding="utf-8"))
    base = f"{lock['app_server']['image']}@{lock['app_server']['image_digest']}"
    image, image_id = integration_probe.build_patched_runtime_image(base)
    labels = json.loads(integration_probe.run([
        "docker", "image", "inspect", "--format", "{{json .Config.Labels}}", image,
    ]))
    result = {
        "image": image,
        "image_id": image_id,
        "base_image": base,
        "source_commit": lock["app_server"]["source_commit"],
        "runtime_patch_sha256": lock["patches"][0]["sha256"],
        "labels_match": (
            labels.get("org.opencontainers.image.revision") == lock["app_server"]["source_commit"]
            and labels.get("io.hekate.runtime-patch.sha256") == lock["patches"][0]["sha256"]
        ),
    }
    if not result["labels_match"]:
        raise RuntimeError("built App Server image labels do not match versions.lock.json")
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
