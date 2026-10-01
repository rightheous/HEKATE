# Runtime guard contract

## Trust and admission

In the Phase 3 integration topology, the private gateway is the only provider route reachable by the pinned local App Server: the App Server runs on an internal Docker network with the gateway as its permitted provider endpoint. The gateway bearer token authenticates that isolated runtime caller; `x-hekate-*` headers and provider request fields are claims that must match durable state. They do not establish task, attempt, registry, provider-agent, conversation, revision, lease, policy, or pricing authority. Production runtime configuration remains closed until verified model, tokenizer, pricing, and routing profiles exist.

For each physical inference request, the gateway loads the admitted operation, immutable envelope and call plan, current registry lease, and configured provider profile from PostgreSQL. It validates the call kind and admitted role/slot, model, positive output-token ceiling, identity claims, operation state, expiry, cancellation, revision, and remaining reservation. Retries are not admitted. Compaction is allowed only up to the persisted compaction-call limit. The gateway does not infer authorization from unused total budget or an in-memory permit queue.

The send boundary is ordered:

1. Authenticate the private caller and accept only the narrow model-discovery or chat-completions route.
2. Strictly parse and size-check the request body and compare its model/output ceiling with the admitted plan.
3. Persist the call intent and permit through `authorize_provider_call`.
4. Recheck the durable binding, lease, cancellation, deadline, and budget through `consume_call_permit`; commit the single-use consume.
5. Only after that commit succeeds, forward the validated request to the fixed configured upstream without redirects.
6. Record provider-call state and usage through the DB inbox and existing accounting services.

A repeated physical call ID cannot consume or forward again. Lost DB confirmation after consumption is not grounds for a second send. Test-only profiles require explicit test mode and a loopback fake upstream. Production settings fail closed without verified model, tokenizer, pricing, and route configuration. No real provider credentials are used by the integration probe.

## Dispatch and observation semantics

Outbox claim and registry lease fences are separate. Before `session.turn`, the worker records a durable send-intent marker in a short transaction after checking the operation and current lease. The bridge call occurs after the transaction closes. A stale claim cannot acknowledge a newer claim, and a job with a send marker cannot be reclaimed for automatic redelivery. An expired claim without a marker can be reclaimed; an expired claim with a marker is held. The probe verifies both sides of this boundary.

An SDK reply means the command was accepted or dispatched. It does not establish provider-call count, provider completion, usage settlement, or Task completion. `events.collect` is bounded polling; an empty result or timeout is not evidence of provider non-execution. Each provider call has its own accounting ID. Only observed terminal/quiescent evidence for every consumed call can close the execution hold. Missing or contradictory usage retains the settlement hold; usage conflicts do not erase previously recorded spend.

Runtime and gateway events enter typed inbox payloads. Applying an event and marking it processed share one database transaction. An exact duplicate of an unprocessed row is still applied during the pending-inbox scan. Exact processed replay is a no-op; conflicting payloads remain auditable. Conflicting terminal outcomes do not replace an already recorded outcome. A pending event can be reprocessed. DB lookup and schema validation do not claim that Evidence references exist or are accessible; Phase 3 does not implement Evidence validation.

If dispatch outcome is uncertain, the call/execution remains RUNNING or UNKNOWN and funds remain held. A new worker, lease fence, or session preparation cannot rebind that execution. Automatic transport resume, retry, operator recovery, and same-execution continuation are not implemented.

## Verification record

The complete focused verification is run with [`scripts/phase3_verify.py`](../../scripts/phase3_verify.py), which invokes [`scripts/phase3_runtime_probe.py`](../../scripts/phase3_runtime_probe.py) after the unit, persistence, bridge, schema, and migration checks. The T1–T7 evidence, command results, and pending rows are in [`p3-20261001T033633Z-63d438e4.json`](../runtime/artifacts/p3-20261001T033633Z-63d438e4.json). That run made zero real provider calls. G7 same-execution resume and G8 exact full-request tokenization remain blocked.
