from __future__ import annotations

from pydantic import TypeAdapter

from .contracts import check_json_payload
from .models import HekateProposal

_PROPOSAL_ADAPTER = TypeAdapter(HekateProposal)


def parse_hekate_proposal(payload: bytes) -> HekateProposal:
    return _PROPOSAL_ADAPTER.validate_json(check_json_payload(payload), strict=True)


def validate_proposal_shape(proposal: HekateProposal) -> None:
    raise NotImplementedError
