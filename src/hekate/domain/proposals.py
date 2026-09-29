from __future__ import annotations

from .models import HekateProposal


def parse_hekate_proposal(payload: bytes) -> HekateProposal:
    raise NotImplementedError


def validate_proposal_shape(proposal: HekateProposal) -> None:
    raise NotImplementedError
