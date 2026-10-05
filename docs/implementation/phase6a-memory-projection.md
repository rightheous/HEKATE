# Phase 6A — Position memory projection

Phase 6A mirrors confirmed PostgreSQL Position versions into the persistent HEKATE's pinned Letta MemFS. PostgreSQL remains authoritative: Task preparation reads the current Position from PostgreSQL, and a Letta projection never completes or changes a Task, Position receipt, or cost ledger.

## Runtime and data contract

The bridge uses the pinned App Server 0.33.8 runtime, SDK 0.8.25, bridge protocol 1, and Node 22.19.0. The runtime patch adds only authenticated `memory.read` and `memory.project` App Server commands. The server verifies the provider agent's owner, creation, registry, role, and persistence tags and writes one server-selected root MemFS file, `hekate_positions.md`. It does not expose a caller-selected path or run a model call. A committed Git read-back supplies the observed version and digest.

Each deterministic entry contains a bounded Position summary, its source version, topic, applicability, confidence, opaque provenance, and an explicit instruction to treat it as untrusted reference data and recheck PostgreSQL. Evidence contents, archive paths, credentials, policies, and runtime configuration are excluded. Entries are limited to 4 KiB, the file to 32 KiB, and the file to eight topics. UTF-8 truncation is explicit.

Position commit writes a durable desired-version record and projection outbox intent in the same transaction as the Position commit. Its separate projection operation records the canonical request hash, parent runtime operation, source and applied base versions, fence, digest, observation, and status. Provider calls, inference permits, and cost accounting are not used. The worker resolves the current Position when it handles a job; it never projects a stale outbox summary. Read/write/read-back occur outside database transactions. The worker advances `applied_version` only after read-back matches the PostgreSQL version and digest.

The App Server serializes operations on its memory repository, rejects stale fences and lower versions, treats an identical version/digest as replay, and rejects a same-version digest conflict. A delayed response is reconciled by reading the current MemFS entry before recording completion. A failed database confirmation remains retryable and does not roll back the external memory file or the PostgreSQL Position.

## Enablement and operations

Projection is disabled by default. The normal HEKATE-only deployment remains unchanged and records new intents as `PENDING_UNSUPPORTED`. To enable the worker path, configure `HEKATE_MEMORY_PROJECTION_ENABLED=true` and run the pinned App Server with MemFS enabled for the persistent HEKATE. Existing unsupported intents are revalidated against the current trusted scope and registry before they become eligible. Do not enable production provider dispatch as part of this feature.

The worker has a dedicated bounded projection handler; inference dispatch, Critic lifecycle, archive deletion, and unsupported jobs remain on their own paths. Temporary lease/hold conflicts stay pending with a retry time. Runtime uncertainty and database confirmation failure preserve the operation and retry after backoff. Position views expose desired/applied and observed versions/digests plus pending reason. Check those values against the PostgreSQL Position before investigating runtime drift; the Letta memory is never the source of truth.

The runtime file is deliberately small and versioned. A format, identity, same-version digest, or read-back conflict is recorded as drift/conflict for operator review; do not clear applied state or change a Position to make a projection pass. Migration `0011_phase6a_memory_projection` refuses downgrade once projection operations or progress exist.

## Verification and handoff

`scripts/phase6a_memory_projection_probe.py` exercises Position v1/v2/v3 and a second topic through real PostgreSQL, the pinned App Server/bridge, and an isolated fake provider. It verifies a v1 commit, actual MemFS read-back, App Server and bridge restart, prior Position in the next Task capsule and separately in the actual provider request, stale-version protection, same-version digest conflict, PostgreSQL claim competition, response-loss reconciliation, confirmation rollback/retry, independent-topic progress, and migration guards. The focused artifact records the exact run and code fingerprint.

The projection itself creates zero inference calls. Fake provider calls in the task scenario are synthetic and explicitly priced as test-only; actual provider calls remain zero. Production dispatch remains blocked. Position projection into Letta is complete for the bounded local MemFS path; full request tokenization (G8), same-execution resume (G7), Critic lifecycle expansion, and Letta memory search remain follow-up work.
