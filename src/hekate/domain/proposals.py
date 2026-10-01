from __future__ import annotations

from pydantic import TypeAdapter

from .contracts import check_json_payload
from .models import HekateProposal

_PROPOSAL_ADAPTER = TypeAdapter(HekateProposal)


def parse_hekate_proposal(payload: bytes) -> HekateProposal:
    return _PROPOSAL_ADAPTER.validate_json(check_json_payload(payload), strict=True)


def validate_proposal_shape(proposal: HekateProposal) -> None:
    if proposal.action == "answer" and not proposal.answer.strip():
        raise ValueError("empty_answer")
    if proposal.action in {"request_information", "abstain"} and (
        not isinstance(proposal.reason, str) or not proposal.reason.strip()
    ):
        raise ValueError("empty_reason")
