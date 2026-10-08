# Phase 5B: Bounded Deliberation

Phase 5B extends the Phase 5A Task workflow with one additional persistent HEKATE reasoning step and one additional review by the existing ephemeral Critic. Control Plane policy, durable PostgreSQL state, and the existing admission/permit path decide whether another step can run. A proposal never selects a provider, model, registry, budget, or executable tool.

## Limits and durable identity

The server-side deliberation configuration is disabled by default. When enabled, the Task-wide caps are one ephemeral Critic, two Critic reviews total, one HEKATE-only continuation, and two syntheses. The initial Phase 5A planning/review/synthesis path keeps its prior behavior. These limits do not reset after a Task revision change.

Migration `0008_phase5b_deliberation_steps` adds `deliberation_steps`, stable step slots, request/proposal hashes, parent attempt/operation/Conclusion links, step attempt/operation/reservation identities, and the Task continuation counter. Database uniqueness constraints prevent a second row for a Task slot, a replayed parent proposal, a duplicated work fingerprint, or a reused operation/reservation identity. Migration `0009_phase5b_parent_attempt_identity` permits a durable child step to name an approved parent attempt before that attempt is admitted. Both migrations preserve existing Phase 4/5A workflow rows and do not create attempts or holds during upgrade.

Continuation approval is part of accepting the parent result transaction. The handler restores the persistent HEKATE identity from the immutable operation binding; validates Task, scope, revision, authorization, current Evidence, context manifest, execution completion, deadline, previous workflow state, allowed `next_action`, concrete purpose fields, duplicate fingerprint, cap, and budget; and then stores the accepted result, step identity, reservation, workflow state, counters, and audit row together. A `critic_review` approval reserves both the Critic review and its following HEKATE synthesis. A `hekate_reasoning` approval gets its own new attempt and operation. Replay returns the existing approved step; changed content on the same identity conflicts.

The provider path remains the existing admission → send intent → current Evidence guard → single-use permit → pinned bridge path. Each step gets a new operation, attempt, reservation, context manifest, and immutable binding. The second Critic review uses the existing Critic registry with a distinct attempt, operation, and new conversation; it receives bounded prior review/dissent context and the current HEKATE candidate. Strict role-specific output contracts prevent a Critic from proposing work or committing a Position. Synthesis sees the accepted review history and durable dissent IDs and remains the only user-facing agent.

## Stops, waits, and cleanup

An exhausted cap or budget uses the accepted HEKATE judgment to write a server response without another model call or Position commit. Deterministic duplicate work ends with `NO_NEW_WORK`; the hard caps end with `ROUND_LIMIT`; unavailable budget ends with `BUDGET`. Invalid proposal, binding, authorization, or permanent Evidence state follows the current-revision `FAILED/POLICY` path. BUSY and lease contention remain retryable. UNKNOWN operation or provider-call state remains held and unretired. Cancellation, deadline, and superseded revision keep their existing convergence semantics. Only proven-unstarted reservations are released.

Critic retirement checks every workflow and deliberation operation plus provider-call quiescence. It uses the durable lifecycle delete intent; failed deletion stays pending and is rechecked after worker restart. Prior conclusions, dissent, provenance, usage, and ledger records remain durable. PostgreSQL is authoritative; Position memory projection remains `PENDING_UNSUPPORTED`.

Standalone deliberation cleanup also runs from worker startup and each maintenance tick. It selects stale-revision, cancelled, expired, or terminal Tasks from durable state, then rechecks Task/scope, the locked step/operation set, parent binding, and accepted-result linkage before sealing. A deferred step is sealed only when its claim is still `CLAIMED/PENDING/NOT_STARTED` with no binding, envelope, or external-start observation. Admitted work must have quiescent execution and call evidence; UNKNOWN stays pending with its reservation and execution hold. Superseded revisions are cleanup-only. Replaying a release does not change its ledger effect or settlement timestamp.

`reject_deliberation_step()` now rolls back its whole Unit of Work if the final WAITING-Task response CAS returns false. Step, workflow, deferred-operation, budget, audit, and retirement-intent effects therefore cannot commit without the Task response. Worker maintenance later applies the normal cancel/deadline convergence and cleanup rules.

## Reproduction

Run from this linked worktree with PostgreSQL on loopback, a database named `hekate_phase5b_*`, and the pinned Node binary:

```sh
uv run --locked python scripts/phase5b_bounded_deliberation_probe.py \
  --database-url "$HEKATE_TEST_DATABASE_URL" \
  --legacy-database-url "$HEKATE_TEST_LEGACY_DATABASE_URL" \
  --empty-migration-database-url "$HEKATE_TEST_EMPTY_MIGRATION_DATABASE_URL" \
  --node-bin /path/to/node-v22.19.0/bin/node
```

Use three distinct loopback databases: the target must be a disposable `hekate_phase5b_*` database, the legacy database may contain existing Phase 5A/5B rows, and the empty migration database must have no Alembic schema. The probe upgrades the target to head `0010`, truncates only that explicitly named target, starts the pinned Letta App Server and bridge, and routes requests only to the local context-checking fake provider. It records the UTC run, database/runtime pins, request payload observations, row identities, accounting, stop cases, and a source fingerprint in a new artifact under `integration/runtime/artifacts/`.

The worktree was seeded from the preserved Phase 4/5A dirty state; the copy manifest is [`p5b-baseline-20261004T090022Z-38167ca2.json`](../../integration/runtime/artifacts/p5b-baseline-20261004T090022Z-38167ca2.json). It identifies copied tracked and untracked source/artifact digests and records that credentials, `.env`, databases, archive data, caches, `node_modules`, and the source Phase 5A worktree were left untouched.

The normal path verifies two Critic Conclusions and dissent records, followed by one atomic Position commit and Task response. The same Critic registry/provider ID uses a new conversation for review two. A PostgreSQL barrier races continuation acceptance; only one reasoning step, operation, and reservation are stored. Caps stop further review/continuation at `ROUND_LIMIT`, and deterministic duplicate work stops at `NO_NEW_WORK` without another inference.

The normal path retained two Critic Conclusions and dissent records, then made one atomic Position commit and Task response. The same Critic registry/provider ID used a new conversation for review two. A barrier held one continuation result under its PostgreSQL row lock while a second processor contended; after release, exactly one reasoning step, operation, and reservation existed, and the continuation counter was one. Cap cases stopped the third Critic review and second HEKATE continuation at `ROUND_LIMIT`; deterministic duplicate work stopped at `NO_NEW_WORK`, all without another inference. The Critic was actually deleted after a failed delete was retried across worker restart; persistent HEKATE remained ready.

The explicit UNKNOWN fixture still has one unresolved Critic review and two reserved envelopes of `$0.036864` each (`$0.073728` combined); no termination or usage was invented. The earlier Phase 4 `ISSUED/ALLOCATED` fixture remains untouched in its separate database/artifact. Position projection remains `PENDING_UNSUPPORTED`. G7 same-execution resume and G8 full-request tokenization remain deferred. Synthetic pricing is test-only and does not validate production pricing or model behavior.

The Phase 5A runtime regression also passed against the Phase 5B schema in [`p5a-20261004T132502Z-d8cdea18.json`](../../integration/runtime/artifacts/p5a-20261004T132502Z-d8cdea18.json), with 15 fake-provider requests and zero real-provider calls. Schema export reproduced `contracts/generated/` byte-for-byte. An isolated empty PostgreSQL database passed upgrade → downgrade → re-upgrade; populated downgrade guards retained their migration head and rows.

## Stop-and-wait maintenance and rollback repair

The stop/wait repair was revalidated by the later full Phase 5B run recorded below. It used PostgreSQL 16.15 and the pinned runtime; all normal and regression calls went only to the context-checking fake provider, with no compactions or real provider calls.

The probe ran the standalone HEKATE continuation cleanup through actual worker startup, maintenance, and restart. The cancellation case ended `CANCELLED/USER_CANCELLED`; the deadline case ended `FAILED/DEADLINE`; both sealed the unstarted step and operation, released the unused `$0.073728` task/system hold once, preserved planning spend, and made no post-gate provider request. Concurrent PostgreSQL maintenance and replay did not add release-ledger entries or audits. The superseded revision's step was sealed and its unused hold released while the revised Task stayed `WAITING` at revision 2; its continuation counter stayed at one and no automatic planning ran.

For the deadline rollback boundary, the probe used the real PostgreSQL deadline predicate with a controlled repository clock. When the final Task-response compare-and-set returned false, the before/after snapshots were identical: the Task remained `WAITING`, its step remained `READY`, the deferred operation remained `CLAIMED`, the reservation remained held, and no response or stop audit was written. A subsequent actual worker pass after deadline convergence failed the Task under the existing `DEADLINE` rule and released the proven-unused hold. The parallel Critic-workflow case also rolled back a false final response CAS, then resumed successfully through the real worker path.

The BUSY/lease case remained pending without extra provider requests or new identities and completed once the lease cleared. The UNKNOWN preservation case is explicitly a synthetic Control Plane fixture, not a simulated provider interruption: UNKNOWN operation/hold remained unresolved and held, with no new request or Critic deletion. The pre-existing Phase 4 `ISSUED/ALLOCATED` fixture remained in its separate database and was not modified. Position projection remains `PENDING_UNSUPPORTED`; G7/G8 remain deferred.

The initial repair fixture ordering was corrected so that the UNKNOWN preservation case runs after the Tasks that need the shared persistent HEKATE. The Phase 5A regression was then run against an isolated PostgreSQL database; its artifact is linked above.

Final local checks after the implementation passed: `uv run --locked python -m compileall -q src scripts tests` and `git diff --check`. The Phase 5B full probe and the Phase 5A regression both ran on isolated loopback databases; the Phase 4 fixture database was not used.

## Maintenance completion and fair retries

Migration `0010_p5b_delib_maint` adds `maintenance_completed_at` and `maintenance_retry_after`. The completion marker is written in the same transaction as step sealing and proven-unused reservation release. Candidate selection excludes marked steps but continues to include incomplete `STOPPED` steps, so a partial cleanup remains recoverable. The migration initializes both fields as null for existing records. Downgrade refuses to discard any non-null maintenance metadata.

Maintenance candidates are ordered with never-deferred work (`maintenance_retry_after IS NULL`) first, followed by deferred rows ordered by their oldest retry timestamp and then the existing Task/step ordering. A candidate with no terminal execution proof is rolled back and deferred for 60 seconds in a separate transaction. The next tick can therefore process new safe work before a due UNKNOWN retry; deferred candidates rotate by retry age after untried work is exhausted. The normal batch remains bounded at 100, and the worker still retries UNKNOWN only as a read/recheck: it does not create a provider request, fabricate quiescence, release its held reservation, or alter its active execution hold. Once a real terminal observation is available, the same step can be selected and cleaned.

Latest maintenance and full Phase 5B evidence: [`p5b-20261004T180912Z-2651b371.json`](../../integration/runtime/artifacts/p5b-20261004T180912Z-2651b371.json). It used PostgreSQL 16.15 at `0010_p5b_delib_maint`, Letta App Server 0.33.8, SDK 0.8.25, bridge protocol 1, and Node 22.19.0. Its source fingerprint is `3ddbc7d8d08998a4a1797c3088cbe59ac854d8de205cf58ffe45d58e6d53082f`. The run made 62 requests to the local fake provider, zero real provider calls, and zero compactions; production dispatch stayed blocked.

The PostgreSQL maintenance scenario inserted 101 already-marked synthetic rows plus a real cancelled continuation target. The target remained the only candidate at limit 100; injected failure before the completion marker left the transaction unchanged; two processors that read the same candidate completed with counts `[0, 1]`; replay returned zero and added no release or audit effects. A second scenario used batch limit 1: an old UNKNOWN was deferred with its reservation and active execution hold preserved, then its retry timestamp was moved due while two never-deferred rows remained. The partially released `STOPPED` row was completed on tick two, the later safe row on tick three, and a fresh maintenance UoW after restart left both completed rows unchanged. The UNKNOWN operation, reservation, and active hold remained unresolved after that restart; assertions checked the hold stayed `UNKNOWN` with no quiescent timestamp. After a synthetic terminal observation, the UNKNOWN row was selected and cleaned once. These terminal-state changes were explicit database test fixtures; they do not simulate a real provider interruption. No post-gate fake-provider request was made by maintenance.

The run preserved two UNKNOWN operations, one unadmitted synthesis step, three reservations with `$0.221184` held, and `$0.005220` already spent in the legacy-row fixture. It did not invent terminal or usage evidence. The isolated legacy clone began at `0009` with 22 Tasks and 23 steps; the `0009`→`0010` upgrade retained 114 operations, 302 ledger rows, both UNKNOWN operations and holds, spend, and held amounts. An empty database passed upgrade→downgrade→re-upgrade, and the populated downgrade guard refused to discard maintenance progress. The UNKNOWN fixture uses one active operation per registry, respecting the database's execution-hold invariant.

## Integrated handoff

### Local operation

Use the existing CLI and worker. The worker requires `HEKATE_DATABASE_URL`, `HEKATE_NODE_BIN` pointing to Node 22.19.0, `HEKATE_LETTA_URL`, `HEKATE_LETTA_TOKEN`, `HEKATE_WORKER_ID`, and a configured `HEKATE_CONFIG_DIR`. Configure bounded Task/system budgets, deadline, HEKATE model/pricing profile, and archive directory in the local policy/settings files; these have no production defaults. Critic and deliberation are disabled in the checked-in policy and require explicit server configuration and a fixed Critic profile.

```sh
uv run --locked python -m hekate worker
uv run --locked python -m hekate evidence import ./source.md \
  --request-key import-source-001 --kind document \
  --retention-class project-source --expires-at 2026-12-31T00:00:00Z
uv run --locked python -m hekate evidence show EVIDENCE_ID
uv run --locked python -m hekate ask --request-key review-001 \
  --topic-id topic-001 --evidence-id EVIDENCE_ID <<'QUESTION'
Review the current source and position.
QUESTION
uv run --locked python -m hekate task TASK_ID
uv run --locked python -m hekate position show topic-001
uv run --locked python -m hekate position history topic-001 --after-version 0 --limit 50
```

### Cumulative delivery

The submitted implementation is cumulative. Phase 4 adds scoped Evidence, immutable Position history/atomic commit, restart reuse, worker Evidence/archive maintenance, and cancellation-safe archive locks. Phase 5A adds one ephemeral Critic review, durable dissent, HEKATE synthesis, safe cleanup, and persistent-HEKATE create-journal reconciliation. Phase 5B adds one bounded HEKATE continuation and one further Critic review, durable admission/counters/holds, stop/wait convergence, and fair standalone cleanup retries.

The Alembic chain is `0001_operations` → `0002_budget` → `0003_runtime_dispatch` → `0004_terminal_inbox` → `0005_phase3b` → `0006_phase4_knowledge` → `0007_phase5a_critic_workflows` → `0008_phase5b_deliberation_steps` → `0009_p5b_parent_attempt_id` → `0010_p5b_delib_maint`. Current head is `0010_p5b_delib_maint`; no existing migration was edited.

Current evidence is split by phase: Phase 4's final dedicated run is [`p4-20261003T165018Z-b67da98c.json`](../../integration/runtime/artifacts/p4-20261003T165018Z-b67da98c.json) (fingerprint `ba0a0474e37e4480ae992a0937eef3500e923bbb07d41e50c327b3d071dd85a1`); archive cancellation is also recorded in [`p4-20261003T122338Z-91d88833-archive-cancel-full.json`](../../integration/runtime/artifacts/p4-20261003T122338Z-91d88833-archive-cancel-full.json). Phase 5A stop/wait and create-journal evidence are [`p5a-20261003T193348Z-40e88886.json`](../../integration/runtime/artifacts/p5a-20261003T193348Z-40e88886.json) (fingerprint `0e175abf1240f330b6b099f188250af75ede1d3cf92f851842b303948d7167a6`) and [`p5a-20261004T081107Z-719917a0.json`](../../integration/runtime/artifacts/p5a-20261004T081107Z-719917a0.json) (fingerprint `7c1a3703528f355eddbe77d9e1403dc6d8bbfba04bf50396e56b74a52e7e1e70`). The later Phase 5A regression is [`p5a-20261004T132502Z-d8cdea18.json`](../../integration/runtime/artifacts/p5a-20261004T132502Z-d8cdea18.json) (fingerprint `40c02d34b1403d27c33d307d848986e15c44f781e00cb4c6db3171182024a18d`). The latest cumulative Phase 5B run and current-source fingerprint are recorded above.

The current Phase 3/3B runtime artifacts are [`p3-20261004T180726Z-b5af8b70.json`](../../integration/runtime/artifacts/p3-20261004T180726Z-b5af8b70.json) and [`p3b-20261004T180742Z-cd3ec2a4.json`](../../integration/runtime/artifacts/p3b-20261004T180742Z-cd3ec2a4.json): both runtime suites passed (9/9 and 8/8). The updated verifier passed 11/11 commands, including the Phase 2 PostgreSQL suite (19 tests), bridge tests/build, schema comparison, Alembic upgrade/check, and both runtime probes. It leaves destructive downgrade/re-upgrade to the isolated Phase 5B migration fixture, which passed above.

The latest cumulative source fingerprint is `3ddbc7d8d08998a4a1797c3088cbe59ac854d8de205cf58ffe45d58e6d53082f`, independently recomputed and equal to the Phase 5B artifact. The fingerprint hashes source changes relative to `HEAD` plus untracked source inputs; committing changes that basis and naturally changes the value. Historical artifacts keep their run-time fingerprints and must not be rewritten as current results.

PostgreSQL remains authoritative. The applied Position-memory watermark is zero and projection remains `PENDING_UNSUPPORTED`; G7 same-execution resume, G8 full-request tokenization, Critic rounds beyond the one added in Phase 5B, repair/retry expansion, and production provider dispatch remain out of scope. Preserve operation/binding identity, evidence guards at permit issue/consume, atomic Position/Task completion, bounded holds, cleanup fences, and UNKNOWN/unsettled accounting in the next phase.
