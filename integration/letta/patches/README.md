# Pinned Letta Code probe patch

`provider-call-context-usage.patch` applies to Letta Code 0.33.8 at commit
`0bb6f741a2cd80159837255a8b23964beeb27e03` and the base image digest in
`../versions.lock.json`. Its SHA-256 is checked by `scripts/integration_probe.py`.

The patch carries the bridge's opaque operation context through the runtime's
user-message metadata to each physical provider call. It creates a fresh
`accounting_call_id` for every attempt, adds the provider response ID and token
usage to successful runtime usage events, and labels compaction calls
separately. A failed physical call emits an accounting-ID-only usage event,
which normalizes to `UNKNOWN` when the provider returns no usage or response ID.
With `HEKATE_REQUIRE_PROVIDER_BINDING=1`, missing or mismatched context fails
before the provider request. The probe gateway then checks the full binding,
operation, call kind, model, output limit, expiry, and one-use call ID before
forwarding.

This is a probe integration patch, not a production authorization token or
budget ledger. The context is bridge supplied and opaque to the model, but it is
not cryptographically signed. Fake provider usage is synthetic and does not
prove provider billing. Keep `overall_status` blocked until the remaining
runtime gates have direct evidence.
