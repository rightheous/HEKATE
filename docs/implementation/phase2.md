# Phase 2: PostgreSQL persistence and budget ledger

## Implemented

Alembic head is `0002_budget`. It creates authorization scopes, tasks and immutable input revisions, attempts, agent registry and leases, unresolved execution holds, operation journal, outbox/inbox, audit and recovery records, budget accounts/reservations, provider calls and permits, immutable usage observations/projections, and an append-only budget ledger. The unimplemented knowledge placeholder is outside the migration chain. The cyclic provider-call/permit foreign key is deferred until transaction commit.

`create_uow_factory(engine)` creates a fresh async session and repositories per unit of work. Application entry points are `tasks.revise`/`tasks.cancel`, `operations.admit_operation`/`record_execution_observation`, and the budget functions `reserve`, `authorize_provider_call`, `consume_call_permit`, `record_call_observation`, `record_usage`, `settle_call`/`settle`, `reconcile_pending`, `list_pending`, and `apply_adjustment`. Operation admission writes the attempt, both account holds and ledger effects, execution hold, outbox dispatch intent, and receipt in one transaction.

The application lock order is operation, authorization scope, task, attempt, registry/lease, then budget accounts in sorted account-ID order. This serializes identity and execution checks before account effects. Every call owns its own UoW; concurrent tasks never share an `AsyncSession`.

## Identity and state boundaries

- An operation ID is idempotent only for the same owner scope and canonical request hash. The hash covers operation kind, binding, envelope, reservation, attempt kind, parent attempt, and payload. Replays return the stored receipt; a changed request conflicts.
- `accounting_call_id` identifies one physical provider call. `(operation_id, slot_key)` allocates distinct calls, including multiple calls with the same kind. `provider_call_id` is nullable metadata. A unique permit is linked to each accounting call and can be consumed once.
- Outbox generations are unique per operation and kind. Claims carry an expiry and increasing fence; stale workers cannot acknowledge or reschedule a reclaimed job. Inbox rows are keyed by provider scope, stable event key, and canonical payload hash, so contradictory payloads remain stored and are flagged.
- Usage observations are append-only and tied to an existing accounting call. The projection fills partial fields without summing observations. Contradictions preserve both observations, mark the projection `CONFLICT`, and pause further automatic settlement.
- `RUNNING` and `UNKNOWN` execution holds block another operation for that registry until a trusted quiescent observation closes the hold. Lease expiry or a new fence does not prove quiescence. Only unallocated reservation capacity is released at quiescence; consumed calls with unknown, incomplete, or conflicting usage keep their hold.

## Budget effects

Amounts use `Decimal` and PostgreSQL `NUMERIC`. Reserving holds the same amount in the task and system accounts and appends one `HOLD` ledger effect per account. Call allocations split that reservation into per-call ceilings. A settled call moves its actual cost from held to spent on both accounts in the same transaction; any unused amount for that call is released. Pending calls retain their allocation. Actual cost above the allocation is recorded without clipping and marks an overrun; available budget can go negative, blocking new approvals.

Usage conflict after settlement does not reverse prior spend. It returns the projection/reservation to pending for reconciliation, while the prior ledger effects remain. Corrections use a separate idempotent `ADJUSTMENT` effect; ledger rows are never updated or deleted.

The detailed test settles a task/system reservation of 8 with two calls costing 5 and 2: task and system each finish at spent 7, held 0. A later conflicting observation leaves spend at 7 and sets the reservation pending. A task-only invoice adjustment of 0.25 changes task spend to 7.25 and is not applied twice.

## Running focused verification

Use a disposable local PostgreSQL 16 database named `hekate_phase2_test`, bound to loopback. The test refuses any other database name or non-local host. Set `HEKATE_PHASE2_TEST_DSN` to that database's local `postgresql+psycopg` URL; keep credentials in the shell environment.

```sh
HEKATE_DATABASE_URL="$HEKATE_PHASE2_TEST_DSN" uv run --locked alembic upgrade head
HEKATE_DATABASE_URL="$HEKATE_PHASE2_TEST_DSN" uv run --locked alembic check
HEKATE_TEST_DATABASE_URL="$HEKATE_PHASE2_TEST_DSN" uv run --locked python -m unittest discover -s tests/persistence -v
```

The PostgreSQL tests reset tables in that dedicated database. They cover rollback, replay/conflict, fenced outbox claims, conflicting inbox payloads, simultaneous reservations, single-use permits, cancel/expiry/fence checks, reconnected `UNKNOWN`, usage completion/conflict/overrun/adjustment, task revision history, and DB failure. Bridge route/build checks run from `bridge/letta` with `npm test` under the repository's Node 24 runtime.

## Phase 3 handoff and remaining gates

The Python DB guard and repositories are not connected to the production TypeScript bridge or a product API/worker loop. Phase 3 must connect permit issuance and consumption to the runtime forwarding boundary, and dispatch to the transactional outbox, without treating an SDK terminal event as provider quiescence. The synthetic test-only price profile cannot authorize production execution. A real model profile, validated tokenizer and full-request token accounting, verified pricing, and production runtime integration remain prerequisites. Same-execution resume after process loss (G7) and exact input-token bounding (G8) remain unresolved; this phase does not change the Phase 1 capability status.

The four source design documents in `docs/design/` were restored from the supplied reference and their manifest hashes were checked. Phase 2 adjusts the detailed design by keeping the unimplemented knowledge migration out of the active head, storing inbox conflicts as separate payload-hash rows, and retaining execution holds until explicit quiescence.
