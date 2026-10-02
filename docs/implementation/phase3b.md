# Phase 3B: one persistent HEKATE task path

## Delivered boundary

The local single-user CLI submits a task through the application service, stores its first input revision and idempotency receipt, and lets the worker prepare and dispatch one HEKATE turn through the existing admission, reservation, outbox, bridge, App Server, and database-backed provider gateway. A valid structured turn output stores its Conclusion and Proposal, waits for both the business result and execution termination, then commits a provisional response. The persistent HEKATE registry is reused across tasks.

The provider gateway ends an SSE response before persisting its final call observation. The observation is stored by a Starlette response background task. A terminal runtime event that arrives first stays durable and pending until individual provider-call termination is recorded. Disconnects and unresolved calls keep an UNKNOWN execution hold. Reprocessing saved results does not resend the SDK turn.

Before admission, the server builds the full `HekateTurnOutput` v1 schema from the strict Python model and embeds it with the server output policy in the turn message. The schema and policy are separate from the Task Capsule data, and the admitted outbox message is forwarded by the bridge unchanged. The policy restricts actions to `answer`, `request_information`, and `abstain`; it requires `conclusion.status = done`, a RegistryId matching the trusted binding, and empty `evidence_used`. It does not use SDK-native structured output or establish answer quality.

Input limits remain explicit: the question can contain at most 65,536 UTF-8 bytes; `max_input_tokens` must be configured as an integer of at least 1 (the Phase 3B fake probe uses 32,768); and the final `session.turn` message is capped at 65,536 Unicode characters. The bridge checks the complete UTF-8 JSONL frame, including its newline, against 1 MiB before admission; a message that exceeds its cap fails the queued Task with `ERROR`, before an operation or permit is created. A one-token setting is only the lowest syntactically accepted configuration, not a usable minimum; the effective minimum that guarantees this prompt fits is unknown until G8 full-request tokenization is resolved. The output token ceiling and call plan are unchanged.

The task view reports execution and cost state separately. `cost_status` is `NOT_DISPATCHED`, `PENDING_SETTLEMENT`, or `SETTLED`; `pending_provider_calls` and `pending_reservations` show unresolved accounting work. A completed response can therefore coexist with pending usage settlement. UNKNOWN execution, pending usage, and a client wait timeout do not authorize an automatic inference retry. An UNKNOWN hold blocks another task from preparing a session for that registry. Cancellation remains `STOPPING` until the operation and every relevant provider call have confirmed terminal states, then becomes `CANCELLED` with outcome `CANCELLED`. A deadline that expires after confirmed execution termination becomes `FAILED` with reason `DEADLINE`. Late results remain `LATE`, with ineligible Conclusions and no stored Task response. Worker startup and maintenance revisit these pending terminal outcomes; stale-revision output cannot terminate the newer revision.

The turn supports `answer`, `request_information`, and `abstain`. Each ends the task in one HEKATE turn. Other proposal actions fail with a deterministic server response. The Conclusion is provisional validation evidence, not factual validation or an authorization credential. Evidence retrieval, Critic, Position commits, automatic repair/retry, and production readiness are outside this phase. G7 same-execution resume and G8 exact full-request tokenization remain unresolved.

## Local configuration and commands

The config directory must contain `local.yaml`, `policy.yaml`, `models.yaml`, and `pricing.yaml`. Configure an already-registered authorization scope in `local.yaml`; do not derive identity from command-line input. Set explicit limits and model/pricing values. The checked-in policy, models, and pricing files are intentionally unconfigured.

```yaml
# local.yaml
identity:
  principal_id: local-principal
  scope_id: local-scope
  policy_version: local-policy-v1
  authz_epoch: 1
```

```yaml
# policy.yaml
limits:
  task_budget_usd: "1.00"
  system_daily_budget_usd: "10.00"
  task_deadline_seconds: 240
```

```yaml
# models.yaml
hekate:
  profile_id: local-hekate-v1
  model: openai-compatible/provider-model
  provider_model: provider-model
  max_input_tokens: 16000
  max_output_tokens: 2048
  max_compaction_calls: 1
```

```yaml
# pricing.yaml
version: local-pricing-v1
prices:
  provider-model:
    input_usd_per_million: "1.00"
    output_usd_per_million: "2.00"
```

The worker also needs `HEKATE_DATABASE_URL`, the pinned Node.js 22.19.0 executable in `HEKATE_NODE_BIN`, a built bridge, `HEKATE_LETTA_URL`, `HEKATE_LETTA_TOKEN`, and a unique `HEKATE_WORKER_ID`. Run a private Letta App Server and route its provider requests through the DB-backed gateway. `build_container()` starts the worker bridge and database connection; it does not start or configure a public API or provider gateway.

```sh
export HEKATE_CONFIG_DIR=/absolute/path/to/config
export HEKATE_DATABASE_URL='postgresql+psycopg://USER:PASSWORD@127.0.0.1:5432/hekate'
export HEKATE_NODE_BIN=/absolute/path/to/node-v22.19.0-linux-x64/bin/node
export HEKATE_LETTA_URL=ws://127.0.0.1:4500
export HEKATE_LETTA_TOKEN='local-private-token'
export HEKATE_WORKER_ID=local-worker-1
PATH="$(dirname "$HEKATE_NODE_BIN"):$PATH" npm --prefix bridge/letta run build
uv run --locked python -m hekate worker
```

In another terminal with the same environment:

```sh
cat question.txt | uv run --locked python -m hekate ask --request-key task-2026-10-01-001 --wait-seconds 30
uv run --locked python -m hekate task TASK_ID
uv run --locked python -m hekate cancel TASK_ID
```

`ask` reads at most 65,536 UTF-8 bytes from stdin. Reusing a request key with the same normalized question returns its original task; changing the question conflicts. On wait timeout, the CLI prints the task ID, current state, and `timed_out: true`; the worker may continue, and `task` retrieves the stored state later. The CLI prints only a response accepted from the database. An oversized final turn message is rejected before admission and provider billing. Cancellation stays `STOPPING` until terminal execution evidence arrives; then the Task converges to `CANCELLED`. Late results remain unaccepted. Invalid or unsupported output fails with a deterministic server response, while unresolved usage remains visible in the separate cost fields.

## Fake-only verification

The pinned integration probe uses dedicated loopback PostgreSQL databases, the locked Letta runtime, and an isolated loopback fake provider. It truncates only `hekate_phase2_test` and `hekate_phase3_test`; never point these URLs at a shared database. It makes zero real provider calls. The full verifier runs the boundary tests, Phase 2 persistence tests, bridge build/tests, schema reproducibility, migration upgrade/check/downgrade/re-upgrade, and both runtime probes:

```sh
export HEKATE_PHASE2_TEST_DATABASE_URL='postgresql+psycopg://USER:PASSWORD@127.0.0.1:55432/hekate_phase2_test'
export HEKATE_TEST_DATABASE_URL='postgresql+psycopg://USER:PASSWORD@127.0.0.1:55432/hekate_phase3_test'
export HEKATE_NODE_BIN=/absolute/path/to/node-v22.19.0-linux-x64/bin/node
export HEKATE_NODE_ARCHIVE=/absolute/path/to/node-v22.19.0-linux-x64.tar.xz
uv run --locked python scripts/phase3_verify.py
```

`scripts/phase3b_single_hekate_probe.py` can be run alone with `HEKATE_TEST_DATABASE_URL`, `HEKATE_NODE_BIN`, and `HEKATE_NODE_ARCHIVE` set. Its JSON artifact records runtime pins, the migration head, scenario results, database identity links, synthetic provider request counts, and cost/hold totals. When run through `phase3_verify.py`, the artifact also records the verifier's commands and results. Synthetic costs are ledger test data, not provider charges.

The latest full verification is recorded in [`p3-20261002T053719Z-9e6c468f.json`](../../integration/runtime/artifacts/p3-20261002T053719Z-9e6c468f.json) and [`p3b-20261002T053735Z-85ecad6a.json`](../../integration/runtime/artifacts/p3b-20261002T053735Z-85ecad6a.json). All 14 verifier commands, 9 Phase 3A scenarios, and 8 Phase 3B scenarios passed with zero real provider calls. The captured provider requests contained the generated schema and matching server schema hash; cancellation and post-execution deadline outcomes survived replay/restart without new inference or accounting effects.
