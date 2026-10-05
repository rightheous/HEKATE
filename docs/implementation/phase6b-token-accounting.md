# Phase 6B: final request token accounting

## Scope and profile

The provider gateway measures the final pinned-runtime chat request after prompt, memory, history, tools, and output schema have been assembled. It strictly parses the bytes, rejects duplicate JSON keys and unsupported input fields, computes input tokens with the checked-in `cl100k_base` asset, checks input/output/context limits, and forwards the same bytes only after the database permit is bound to their digest and the immutable execution-profile digest.

The executable profile is intentionally test-only: `hekate.fake.chat-json.v1`, `tiktoken==0.14.0`, the checked-in tokenizer asset SHA-256 `223921b76ee99bde995b7ff738513eef100fb51d18c93597a113bcffe865b2a7`, a fixed renderer revision, and synthetic Decimal pricing. The renderer includes messages (including tool calls/results), tool declarations, tool choice, and structured-output schema. The SDK's `store` request control is accepted only as a boolean and does not enter the token document. Unsupported and multimodal fields fail closed. Asset and renderer digests are checked locally; no tokenizer download occurs during request handling.

The model, tokenizer, renderer, limits, pricing semantics, and evidence digests contribute to the profile content digest. `ProviderCallPlan`, provider-call rows, and call-permit rows bind to that digest. The gateway records the SHA-256 of the original request bytes, measured input tokens, requested output ceiling, context window, tokenizer/renderer identity, pricing digest, and measurement state. `LEGACY_UNMEASURED` rows keep SQL NULL measurement data; historical token counts are not reconstructed.

Preflight input measurement is audit and admission evidence, not provider usage. Usage observations continue through the existing settlement path. Unsupported cache/reasoning usage dimensions are not priced as ordinary input/output; their pending settlement and hold remain visible.

## Legacy settlement compatibility

Input-request measurement and usage pricing are separate contracts. A pre-0013 call with `measurement_status=LEGACY_UNMEASURED` can settle from its persisted aggregate input/output usage without inventing request bytes, token measurements, digests, or verification claims. This compatibility is limited to `test_only=true` rows whose saved pricing version is one of the previously used synthetic contracts: `test-price-v1`, `test-prices-v1`, `synthetic-v1`, `local-pricing-v1`, `phase3b-synthetic-v1`, `phase4-synthetic-v1`, `phase5a-synthetic-pricing-v1`, `phase5b-synthetic-pricing-v1`, or `phase6a-synthetic-pricing-v1`. The saved model must be nonempty, both persisted rates finite and nonnegative, usage complete and conflict-free, execution quiescent, and cache/reasoning counts absent or zero.

The legacy path interprets those known test-only rates as aggregate input/output pricing only while calculating settlement. It leaves call and permit measurement columns untouched. A non-test legacy call, unrecognized version, invalid saved rate, incomplete/conflicting usage, unresolved execution, or unsupported usage dimension remains pending with its reason and hold. `MEASURED` calls must still have request/profile/pricing metadata that matches the frozen call binding; missing or changed metadata never falls back to legacy pricing. Existing provider-reported cost settlement is unchanged. No schema migration was needed.

The focused PostgreSQL probe is `scripts/phase6b_legacy_settlement_probe.py`. The 2026-10-05 run `integration/runtime/artifacts/p6b-legacy-20261005T144610Z-3bf23853.json` settled the known legacy fixture at `$0.00012` in task and system accounts, released both holds, preserved NULL measurement fields, and did not add ledger effects on replay. Concurrent PostgreSQL settlement created one pair of SETTLE entries; injected failure rolled back the account, projection, reservation, and ledger changes before a successful retry. UNKNOWN execution, incomplete/conflicting usage, unsupported dimensions, unrecognized pricing, and non-test legacy rows remained pending with existing nonzero spend and holds unchanged. MEASURED settlement and request/profile permit binding were also exercised. The existing Phase 2 PostgreSQL persistence suite passed 20 tests against the isolated database. The probe made zero fake-provider and real-provider requests.

## Production status

`config/models.yaml` and `config/pricing.yaml` remain unconfigured for production. A YAML verification boolean cannot enable dispatch. No production model or route was selected, and no external provider request was made. TEST_CONTRACT_VERIFIED covers only the fake chat contract; G8 production remains blocked. G7 same-execution resume also remains deferred.

Before production G8 can be considered, the next stage needs evidence for:

1. The selected provider/model and immutable revision, context window, and token ceilings.
2. The exact API framing used by the pinned Letta runtime for system/developer/user/assistant messages, memory, history, tool calls/results, tool schemas, structured output, and compaction.
3. A pinned tokenizer and renderer implementation whose output has been compared against the selected provider's accepted request semantics, including representative long and structured requests.
4. Effective-dated pricing and the provider's usage definitions for input, output, cache, and reasoning tokens.
5. Separate approval, limits, and rollback steps for any paid external verification request.

Until those facts are verified and bound into a new immutable profile, production dispatch stays blocked.

## Validation

The Phase 6B runner executes the focused PostgreSQL/fake-gateway boundary probe, the Phase 6A pinned-runtime memory/read-back regression, and the Phase 5B deliberation/UNKNOWN-hold regression. It creates a separate JSON artifact for each child and a Phase 6B summary. The summary contains source, config, renderer-contract, and tokenizer hashes; child DB/runtime identities; per-call measurement evidence; and explicitly marked deferred production checks.

The recorded focused cases cover an exact input/context boundary, input and context overflow, system/memory/history/tool/schema overflow, unsupported input, changed request replay, and profile digest substitution. The pinned-runtime regression checks that gateway request-byte digests and gateway token counts match independent observations at the fake provider. The compaction regression verifies its separate call/permit and independent token measurement. Migration checks cover empty upgrade/downgrade/re-upgrade and refusal to discard measured request history from a populated database.

The final summary artifact is written under `integration/runtime/artifacts/p6b-<UTC>-token-accounting.json`; its actual path is printed by the runner and linked in the completion report. The successful runtime children used for the current execution are `p6b-20261005T134246Z-ce252eee-phase3.json`, `p6b-20261005T134246Z-ce252eee-phase6a.json`, and `p6b-20261005T134246Z-ce252eee-phase5b.json`. They passed against isolated PostgreSQL databases and the pinned App Server. The first summary assembly exposed an incorrect assumption about where the Phase 5B report records its production-dispatch status; the corrected summary was assembled from the completed children.

The Phase 3 verifier was also run after this Phase 6B implementation. Its first attempt exposed a stale Phase 2 test assertion that expected migration `0010` even though the database was upgraded through `0013`. The persistence test now compares the health result with the actual `alembic_version` row. The verifier was rerun successfully: 11/11 commands, 10/10 Phase 3 runtime scenarios, and 8/8 Phase 3B scenarios passed. Artifacts: `p3-20261005T135809Z-adb3ad1e.json` and `p3b-20261005T135833Z-795ff18f.json`.

Re-run with isolated loopback PostgreSQL databases and the versions pinned in `integration/letta/versions.lock.json`:

```sh
PATH=/tmp/hekate-node-v22.19.0/bin:$PATH uv run --locked python scripts/phase6b_token_accounting_probe.py \
  --phase3-database-url 'postgresql+psycopg://USER:PASSWORD@127.0.0.1:55436/hekate_phase3_test' \
  --phase6a-database-url 'postgresql+psycopg://USER:PASSWORD@127.0.0.1:55436/hekate_phase6a_runtime' \
  --phase6a-empty-migration-database-url 'postgresql+psycopg://USER:PASSWORD@127.0.0.1:55436/hekate_phase6a_empty' \
  --phase5b-database-url 'postgresql+psycopg://USER:PASSWORD@127.0.0.1:55436/hekate_phase5b_runtime' \
  --phase5b-empty-migration-database-url 'postgresql+psycopg://USER:PASSWORD@127.0.0.1:55436/hekate_phase5b_empty' \
  --node-bin /tmp/hekate-node-v22.19.0/bin/node \
  --node-archive /tmp/hekate-node-v22.19.0-linux-x64.tar.xz \
  --phase3-verifier-artifact integration/runtime/artifacts/p3-20261005T135809Z-adb3ad1e.json
```

No bridge protocol or JSON Schema contract changed. The Phase 6B migration is `0013_provider_token_measurement`; existing migration files were not edited.
