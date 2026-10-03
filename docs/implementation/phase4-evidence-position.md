# Phase 4: Evidence and Position

Phase 4 adds local Evidence import and scoped reads, immutable Position history, and an atomic Position commit path for the persistent HEKATE runtime. PostgreSQL is the authoritative source for current Positions. The Letta memory projection remains pending and is not executed.

## Runtime path

`hekate evidence import FILE` accepts a UTF-8 local file up to 1 MiB. Scope and access epoch come from the configured actor and authorization row; the input `access_scope` value is ignored. The importer checks the supplied digest against the bytes, writes the content-addressed archive object outside the database transaction, then marks the staged artifact and Evidence available. A failed archive write leaves staged state that can be retried. Import requires a request key, kind, retention class, and explicit expiry.

Evidence records retain source location, retrieval and observation times, derivation edges, computed root source IDs, access epoch, content version, retention class, and expiry. Expiry first marks Evidence unavailable and records a durable artifact deletion intent. Deletion then runs outside the transaction and can be retried. Expired Evidence metadata remains available to historical Position references; the source input file is not deleted.

Task submissions include optional `topic_id` and `evidence_refs` in their request identity and input revision. During preparation, the worker snapshots the current Position version and reads the selected Evidence under the current actor scope. It sends bounded excerpts and provenance in `TaskCapsule.task_data`, with a notice that Evidence is untrusted task data. Total Evidence excerpts are capped at 32 KiB. A context manifest records the selected Evidence IDs, content versions, access epochs, and Position version. The admission frame byte check remains an admission limit, not an input-token guarantee.

The preparation, provider-send, result-adoption, and commit paths check the current authorization and Evidence state. Evidence expiry or authorization changes before provider dispatch prevent a request from reaching the provider. Text-only legacy runtime operations without a topic or Evidence remain compatible; a Task with topic or Evidence requires its context manifest.

## Position commit and reuse

The server assigns the commit operation ID as `<runtime_operation_id>:position.commit`. A successful commit locks the authorization scope, Task, HEKATE registry, topic, and sorted references; inserts the immutable Position version and Evidence/dissent links; updates the current pointer; stores audit, receipt, projection intent, and Task response; and completes the Task in the same transaction. Replays with the same actor and request return the original receipt. A stale base version records a conflict and completes the Task as `NEEDS_USER_INPUT`; it does not rebase or invoke another inference.

Conclusion-local objection IDs are stored as durable dissent IDs scoped to the Conclusion. Later Tasks receive the saved Position and its dissent content. After worker and bridge restart, the next Task reads the current Position from PostgreSQL and uses it as `targeted_review` context.

Position versions, Evidence links, dissent links, and dissent records have database immutability triggers. A commit creates a durable projection row and outbox item with `PENDING_UNSUPPORTED` and `memory_projection_not_implemented`. The runtime dispatch worker claims only `dispatch` outbox items; no Letta memory projection is attempted and the applied watermark remains zero.

## CLI

```sh
hekate evidence import ./source.md \
  --request-key import-source-001 \
  --kind document \
  --retention-class project-source \
  --expires-at 2026-12-31T00:00:00Z
hekate evidence show EVIDENCE_ID
hekate ask --request-key review-001 --topic-id topic-001 --evidence-id EVIDENCE_ID
hekate position show topic-001
hekate position history topic-001 --after-version 0 --limit 50
```

## Verification

The current Phase 4 run is recorded in [`p4-20261002T084850Z-78ad2a7d.json`](../../integration/runtime/artifacts/p4-20261002T084850Z-78ad2a7d.json). It is based on `675cadfb1606884c6b7d3ff32bec2b8bb2b1630e` and records the current source fingerprint. It used PostgreSQL 16.15, migration `0006_phase4_knowledge`, the pinned Letta App Server 0.33.8, SDK 0.8.25, bridge protocol 1, and Node 22.19.0. Ten Phase 4 scenarios passed: Evidence scope and replay, Position commit and replay, restart reuse/history, immutable Position rows, concurrent base-version conflict, transaction rollback, cancellation/revision/deadline, stale authorization, version conflict, and Evidence expiry/archive retry. There were eight fake-provider HTTP requests and zero real provider calls. The artifact records eight settled provider calls and zero unresolved or unsettled calls.

The first saved Position uses the database's composite identity `(scope, topic_id, version)` with version 1; after the worker and bridge restart, the next Task received that version and committed version 2. The artifact records both identities, Task and operation IDs, registry ID, Evidence IDs, both versions' provenance and Evidence/dissent links, and the captured provider request context. PostgreSQL remained authoritative: the latest projection desired version is 2, applied version is 0, and projection work remains `PENDING_UNSUPPORTED` with `memory_projection_not_implemented`. No projection call ran.

PostgreSQL metadata comparison passed. An isolated empty database passed downgrade-to-base and re-upgrade; downgrade of the populated Phase 4 database was refused and its migration head remained `0006_phase4_knowledge`. Six generated schemas matched the Python generator byte-for-byte. The latest source compile, `git diff --check`, the 15 source-schema contract tests, and six Phase 3 bridge boundary tests passed after the probe was updated to record Position identities. The schema test compares all six checked-in schemas directly with generator output.

The full Phase 3 verifier passed 14/14 commands, 9/9 Phase 3A scenarios, and 8/8 Phase 3B scenarios in [`p3-20261002T083636Z-6894ae6e.json`](../../integration/runtime/artifacts/p3-20261002T083636Z-6894ae6e.json) and [`p3b-20261002T083652Z-7690ced3.json`](../../integration/runtime/artifacts/p3b-20261002T083652Z-7690ced3.json). That full run preceded the final additive change making `TargetPosition.body` and `provenance` optional for the original v1 example. The affected source-schema tests, schema comparison, Phase 3 boundary tests, and the complete Phase 4 runtime probe passed afterward. No real provider calls were made; production dispatch remains closed.

## Handoff

- Implement a separately claimed and acknowledged Letta memory projection handler only after its wire contract is defined. Keep PostgreSQL authoritative and preserve pending state on failure.
- Address G7 same-execution resume and G8 exact full-request tokenization in their planned phases.
- Keep Critic execution, automatic continuation, external retrieval, production dispatch authorization, and HTTP product APIs out of this phase.
