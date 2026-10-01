# Phase 3: runtime dispatch and provider-call accounting

## Delivered boundary

Phase 3 connects PostgreSQL admission and reservations to the pinned Letta runtime, a private JSONL bridge, and a DB-backed provider gateway. Each physical provider request must have its own persisted call identity and single-use permit. Permit consumption commits before the gateway forwards the request. Runtime and provider observations enter the inbox and update call state and usage through application repositories.

An outbox acknowledgement means the SDK accepted the turn command. It does not mean a provider call completed, usage settled, or the Task completed. A send intent is persisted before the bridge call. An uncertain send, missing usage, or database failure keeps the corresponding call or execution hold pending; the worker does not resend it automatically. A registry with a RUNNING or UNKNOWN execution hold cannot prepare a replacement session or claim another operation.

The gateway trusts neither `x-hekate-*` headers nor request body metadata as authority. It reconstructs the operation binding, envelope, call plan, current registry lease, and provider profile from PostgreSQL and server configuration. It accepts only authenticated requests for model discovery or the configured chat-completions route, validates the forwarded body and call role, and uses a fixed upstream URL. Redirects and arbitrary upstreams are rejected. Test profiles are synthetic and restricted to loopback. Production dispatch stays closed until verified model, full-request tokenizer, pricing, and routing configuration exist.

## Pinned runtime and reproduction

The integration probe reads `integration/letta/versions.lock.json` and verifies these pins:

| Component | Pin |
|---|---|
| Node.js | 22.19.0; archive SHA-256 `c0649af18e6a24f6fe5535a3e86b341dd49a8e71117c8b68bde973ef834f16f2` |
| Letta Agent SDK | 0.8.25 |
| Letta Code App Server | 0.33.8, source `0bb6f741a2cd80159837255a8b23964beeb27e03` |
| App Server protocol | 1 |
| Runtime patch | `provider-call-context-usage.patch`, SHA-256 `e45bab7a65db8710d1e0e79573547faa4c7ef4ba16816367629cb6fbd7b0ee6f` |

The verification runner needs Docker, the pinned Node archive/runtime, and two disposable PostgreSQL 16 databases on loopback: `hekate_phase2_test` and `hekate_phase3_test`. The Phase 2 suite truncates its database; the runtime probe migrates and truncates the Phase 3 database. Do not point either URL at a shared or production database. The Phase 2 implementation notes record Node 24 for that phase's bridge checks; this Phase 3 run uses the locked Node 22.19.0 runtime.

```sh
export HEKATE_PHASE2_TEST_DATABASE_URL='postgresql+psycopg://USER:PASSWORD@127.0.0.1:55432/hekate_phase2_test'
export HEKATE_TEST_DATABASE_URL='postgresql+psycopg://USER:PASSWORD@127.0.0.1:55432/hekate_phase3_test'
export HEKATE_NODE_BIN=/path/to/node-v22.19.0-linux-x64/bin/node
export HEKATE_NODE_ARCHIVE=/path/to/node-v22.19.0-linux-x64.tar.xz
uv run python scripts/phase3_verify.py
```

The runner records each executed command and its result in the integration artifacts. It upgrades both disposable databases, runs Python compilation, the focused Phase 3 boundary tests, the Phase 2 PostgreSQL suite, bridge build/tests, byte-for-byte schema regeneration, migration upgrade/check/downgrade/re-upgrade, `git diff --check`, and both pinned Phase 3A and Phase 3B runtime probes. Python dependency resolution uses the checked-in `uv.lock`.

The application entry points available for Phase 3A are `build_container()`/`close_container()` in `bootstrap.py`, `run_worker()`/`dispatch_job()`/`process_pending_inbox()` in `worker/service.py`, `BridgeClient`, and `create_provider_gateway()`. Phase 3B adds the local `ask`, `task`, `cancel`, and `worker` CLI path, documented in [`phase3b.md`](phase3b.md). There is still no production provider profile or public API configuration, so production settings fail closed. Runtime abort/recover/memory projection and `tasks.complete()` remain unsupported.

Application transactions follow the Phase 2 lock order: operation, authorization scope, task, attempt, registry/lease, then budget accounts sorted by ID. Each bridge, gateway, worker event, and repository application uses its own UoW; subprocess, SDK, and HTTP I/O happen after the database transaction closes. Worker shutdown stops new claims, drains for at most five seconds, marks uncertain active work UNKNOWN where the DB is available, and closes the bridge. Startup scans pending inbox rows. Existing send-intent work is never automatically resent.

## Integration evidence

The T1–T7 run and verification command log are recorded in [`p3-20261001T033633Z-63d438e4.json`](../../integration/runtime/artifacts/p3-20261001T033633Z-63d438e4.json). It used PostgreSQL 16.15 at migration `0003_runtime_dispatch`, the pinned runtime above, an isolated fake provider, and made **zero real provider calls**. All 9 verification commands and all 9 runtime scenarios passed; the command log includes 3 Phase 3 tests, 19 Phase 2 PostgreSQL tests, 15 bridge tests, and 5 generated-schema comparisons.

The final Phase 3A/3B verification is recorded in [`p3-20261001T073347Z-916d84bd.json`](../../integration/runtime/artifacts/p3-20261001T073347Z-916d84bd.json) and [`p3b-20261001T073404Z-58d4681b.json`](../../integration/runtime/artifacts/p3b-20261001T073404Z-58d4681b.json). All 14 verification commands, 9 Phase 3A scenarios, and 7 Phase 3B scenarios passed with zero real provider calls.

| Check | Result |
|---|---|
| T1 DB admission through runtime, gateway, fake provider, inbox, and settlement | Pass; one permit and one fake request; repeat collection did not resend or settle twice; runtime toolset was empty. |
| T2 runtime compaction and role/retry limits | Pass; a separate runtime exercise produced one `turn` and one `compaction`, each with its own accounting ID and consumed permit. Unauthorized role use and retries were denied. |
| T3 outbox claims, fencing, and durable send marker | Pass; two workers had one winner, an expired pre-send claim was reclaimed, stale owners could not set intent or ACK, and an expired post-marker job could not be reclaimed. |
| T4 App Server loss after dispatch | Pass; execution remained UNKNOWN with a consumed call pending. A terminal SDK observation alone could not close the provider call. A new DB connection could not claim work or prepare another runtime session. |
| T5 permit and DB failures | Pass; authorize/consume storage failures, revision/cancel changes between authorization and consume, invalid binding, and replay all caused zero forward. A simulated observation-write failure after one forward kept the call and consumed permit pending; replay caused no second forward. |
| T6 inbox replay and conflict | Pass; a duplicate of an unprocessed inbox row was processed on scan, terminal replay was a no-op, a conflicting terminal outcome was preserved without replacing success, and usage conflict did not reverse prior spend. Delayed fake usage after cancellation settled the original call without creating a new call or operation. |
| T7 private gateway boundary | Pass; wrong bearer returned 401, unsupported routes returned 404, external test routing was rejected, and production settings without a verified profile failed closed. |

The run observed 20 gateway requests (7 responses with 200, 1 with 401, 10 with 402, 1 with 404, and 1 with 500), 7 fake-provider forwards, 12 DB provider-call rows and permits, 8 consumed permits, and 20 processed inbox rows. Eight call rows intentionally remained pending: one usage conflict, one consumed call with no confirmed forward, one revoked permit, one UNKNOWN/RUNNING call after App Server loss, one post-forward observation-write failure, and three unconsumed permits proven not to have forwarded. The post-forward failure was injected at the observation persistence boundary; the consumed call stayed pending and replay forwarded zero additional requests.

Synthetic ledger totals were 18 HOLD, 4 RELEASE, and 10 SETTLE entries. The synthetic system account ended at `$0.000225` spent and `$69.999680` held. The T2 task's late usage after cancellation settled against its original accounting call; the remaining holds correspond to deliberately unresolved or proven pre-forward fault cases. These values are probe data, not real provider charges.

## Remaining gates and scope

G7 (resuming the same execution after transport loss) and G8 (validated exact tokenization of the full effective request) remain blocked. The pinned runtime patch supplies per-call identity for this test path; stock SDK capability flags remain false. No real provider profile or production readiness is claimed.

User HTTP ingress, Critic lifecycle, Position/Evidence persistence, memory projection, automatic retry/resume, and operator recovery remain outside Phase 3. The local single-HEKATE answer loop is implemented in Phase 3B, while G7/G8 remain prerequisites for production dispatch.
