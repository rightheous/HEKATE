from __future__ import annotations

from hekate.domain.models import ProjectionJob, ProjectionReceipt, ProjectionStatus
from hekate.domain.types import RegistryId, TopicId


async def project_position(job: ProjectionJob) -> ProjectionReceipt:
    raise NotImplementedError


async def verify_projection(
    topic_id: TopicId, registry_id: RegistryId
) -> ProjectionStatus:
    raise NotImplementedError
