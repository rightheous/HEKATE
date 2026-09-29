from __future__ import annotations

from hekate.domain.models import SafeEvent
from hekate.settings import Settings


def redact_event(event: object) -> SafeEvent:
    raise NotImplementedError


def record_metrics(event: object) -> None:
    raise NotImplementedError


def configure_logging(settings: Settings) -> None:
    raise NotImplementedError
