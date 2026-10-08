# Phase 6C: local Ollama/Qwen request measurement

## Implemented path

`config/local-candidates.yaml` is a disabled, content-addressed identity record for the installed
`orcarouter/Qwen3.8-27B-Uncensored:iq4_xs` model. It is separate from `config/models.yaml` and
`config/pricing.yaml`, which remain unconfigured for normal runtime dispatch. The candidate binds
Ollama 0.34.0, the manifest and GGUF blob identities, tokenizer arrays, Qwen3.5 renderer and parser,
the request normalizer, and an explicitly local external-tariff policy. Its `dispatch_approved`,
`runtime_context_verified`, and `inference_usage_verified` states remain false.

`qwen_ollama.py` loads the checked-in tokenizer-only metadata and Unicode property asset after
checking their SHA-256 values. The tokenizer ports the installed GPT-2 byte mapping, Qwen3.5
Unicode segmentation, llama.cpp b10760 ordered BPE merges, and special-token parsing. The renderer
ports the Ollama 0.34.0 Qwen3.5 renderer, including Go whitespace trimming, developer-to-system
normalization, assistant history, generation prefix, and the explicit no-thinking prefix. HEKATE
embeds its generated output schema in the actual turn prompt, so the renderer measures those bytes.
The candidate disables the pinned SDK's portable `StructuredOutput` tool because the candidate
allows no tools. The bridge still validates the returned assistant JSON against the output schema.
The schema is sent in the prompt rather than as an OpenAI `response_format` grammar. Unsupported
multimodal blocks, tool use, multiple completions, literal control tokens in message text, and
unrecognized request fields fail closed.

The pinned OpenAI-compatible SDK request includes `store=false`. Ollama v0.34.0's
`openai.ChatCompletionRequest` has no `store` member, so the Qwen normalizer accepts only the explicit
false value and removes it before hashing and forwarding the final Ollama request.

The provider gateway selects Qwen measurement for the `ollama-local` profile. It hashes the normalized
bytes that it forwards, measures tokens from the rendered prompt, checks the input/output/context
ceilings, and binds the request digest and profile digest to the same PostgreSQL call and single-use
permit. This path uses the installed GGUF-backed Qwen tokenizer rather than the cl100k fake-chat
tokenizer. `store=false` is the only accepted SDK store value; it is removed because the pinned Ollama
ChatCompletionRequest has no store field. The preflight count is not provider-reported usage. Qwen execution profiles are
`test_only=true`; production gateway construction continues to reject them. The ordinary fake chat
profile and Phase 6B `LEGACY_UNMEASURED` settlement path remain separate.

`scripts/phase6c_ollama_qwen_probe.py` supports two distinct modes. The default verifies live
metadata and runs one pinned App Server turn through PostgreSQL and the isolated fake provider; the
fake validates the generated Hekate output schema, Task Capsule, policy, model, no-thinking setting,
and call ceiling before returning a synthetic result. The explicit
`--execute-local-ollama --confirmed-effective-context-tokens 8192` mode targets only the existing
loopback tunnel at `127.0.0.1:19191` and is a one-Task, one-call smoke route. It requires a separate
fresh database named `hekate_phase6c_smoke_*`, a human-verified effective 8192-token context, a
zero-compaction test profile, and the explicitly supplied execution flag. No alternate provider or
hosted endpoint is configured. Errors do not trigger retries or another Task attempt.

## Identity and offline evidence

The read-only live metadata preflight uses exactly six routes: `GET /api/version`, `GET /api/tags`,
`GET /api/ps`, `GET /v1/models`, and `POST /api/show` with `verbose=true` and `verbose=false`. It
compares the version, model tag and manifest digest, installed tokenizer arrays, template digest,
and exact OpenAI model discovery. The `/api/tags` manifest digest, GGUF blob digest, tokenizer
array digests, and renderer source digest are recorded as different identities.

The current observed model has manifest SHA-256
`84e6355d6764e264ccdfe486243821e7000eaff08827557af4e3dc537c772c2a`, GGUF blob SHA-256
`ce18b852ff0f7f7fc3cbe3467b4a87d3b27e7b3e611bf41e0a529220604aa79f`, architecture `qwen35`,
27,320,697,856 parameters, and IQ4_XS quantization. The checked tokenizer is the installed model's
GGUF material, not a similarly named public tokenizer. Its token, merge, and token-type array hashes
are pinned in the candidate and asset. The Qwen3.5 renderer source is pinned to Ollama v0.34.0 and
independently compared with the official Go renderer. The BPE token IDs were compared on 45
deterministic strings against a CPU-only llama.cpp b10760 tokenizer; the checked fixtures preserve
representative vectors and five rendered prompts.

`/api/ps` was empty at observation time. The model metadata's 262,144 context value is not an
effective loaded context claim. The provider gate uses an 8,192-token context cap, with at most 6,144
input and 2,048 output tokens. The pinned Letta App Server agent receives a separate 16,384-token
context-estimator setting. Its `pi-ai` output heuristic reserves 4,096 tokens from that estimate; this
headroom keeps a roughly 5,957-token prompt from reducing the requested output cap to 1 token. The
gateway still rejects requests whose exact Qwen input plus output exceeds 8,192. This estimator does
not widen the provider gate or establish Ollama's loaded context. The request includes the full rendered prompt and
assistant generation prefix; `reasoning_effort=none` is normalized before digesting and maps to
Ollama's `Think(false)` path. The 2,048 output ceiling leaves room for the structured
`HekateTurnOutput` answer, conclusion, confidence, and provenance fields. The current server has
not been observed with the model loaded at that context, so the 8,192 cap still requires separate
effective-context confirmation before any actual smoke call.

The bridge sends the App Server context-estimator setting separately when it creates an agent. The
pinned Letta agent stores it as `model_settings.context_window_limit` and the output ceiling as
`model_settings.max_tokens`; the 6,144 input ceiling remains enforced by request measurement and the
database permit. Qwen `session.prepare` retains its output contract while disabling SDK
`outputFormat`; bridge-side schema validation remains strict. Existing profiles keep SDK output
formatting by default. The
focused runtime probe reads these settings from the isolated App Server's agent record. A real smoke
must use a fresh isolated scope so an existing persistent HEKATE with older settings is not mistaken
for a reconfigured agent.

The candidate's external API tariff is zero only because of
`local-candidate-policy-v1`; Ollama did not report a cost of zero. GPU, power, and host costs are
excluded and unmeasured. Local provider usage semantics, output counts, and any streaming usage
fields remain unverified until a real smoke response is observed. Missing or conflicting usage must
remain pending/UNKNOWN and must never be filled from the Qwen preflight input count.

## Actual-model smoke conditions

The dedicated local smoke mode is **not run in Phase 6C**. Before running it, an operator must
independently verify the tunnel and service configuration, confirm the Ollama effective context is
at least 8,192 tokens, and review the live metadata preflight. The metadata context value alone is
not sufficient. The smoke uses a fresh isolated PostgreSQL database, the pinned App Server and
bridge, one short public prompt, one persistent HEKATE, no Critic or deliberation policy, no
compaction, no retry, a 2,048-token output cap, and the Task's 240-second deadline. It goes through
the gateway and database permit. The provider request is not sent directly from Letta to Ollama.

The prepared command shape is:

```sh
PATH=/tmp/hekate-node-v22.19.0/bin:$PATH uv run --locked python scripts/phase6c_ollama_qwen_probe.py \
  --database-url 'postgresql+psycopg://<local-test-user>:<local-test-password>@127.0.0.1:<isolated-port>/hekate_phase6c_smoke_<unique-id>' \
  --node-bin /tmp/hekate-node-v22.19.0/bin/node \
  --node-archive /tmp/hekate-node-v22.19.0-linux-x64.tar.xz \
  --execute-local-ollama --confirmed-effective-context-tokens 8192
```

This is a local smoke command, not production enablement. The command was not executed during this
work. No Ollama chat, generate, or completions request was sent, and no model was loaded or changed.
The script is deliberately one-call bounded: its admitted call plan has one main call, zero
compactions, and zero retries. If the upstream result is unknown or the output is invalid, the
probe must preserve that state and stop.

## Remaining boundaries

- The current fake-provider runtime turn proves that the pinned Letta request shape passes through
  Qwen normalization, the GGUF-backed Qwen token measurement, the PostgreSQL permit, and the exact
  forwarded bytes. It verifies the prompt-embedded schema, absence of SDK structured-output tools,
  and an output cap above the fake response's reported usage.
- It does not prove a generated answer from Qwen, loaded context, Ollama's response usage semantics,
  output compatibility, or GPU/energy cost.
- Production remains blocked because the candidate is not dispatch-approved and normal model and
  pricing configuration remains unset. Test-only profile acceptance requires explicit test-mode
  construction; a metadata result or environment variable does not change this state.
- G8 production exact-request validation remains incomplete pending actual provider/runtime usage
  evidence and production approval. G7 same-execution resume remains a separate deferred boundary.
- Phase 6B legacy settlement compatibility is preserved; legacy rows remain unmeasured rather than
  receiving reconstructed digests or token counts.

## Focused validation

Run the focused fake-runtime probe against a newly created loopback PostgreSQL database with the
current Phase 6B migration head. It checks live metadata identity, the pinned App Server request,
the Qwen request/profile digest in PostgreSQL, consumed permit, one-call/zero-compaction bounds,
synthetic zero-tariff accounting, and same-key replay after worker restart. It sends no request to
the Ollama inference endpoints. It also checks the stored Letta context and output settings before
evaluating the turn. If the configured context is too small for the rendered request, the gateway
rejects it before upstream; that is not a successful Qwen smoke.

The Phase 6B legacy settlement probe remains a separate compatibility check against its own fresh
isolated PostgreSQL instance. Its UNKNOWN, incomplete, conflicting, and legacy-unmeasured rows are
preserved rather than normalized.

## Verification record

The pinned fake-runtime integration path completed a Task with one fake-provider request, no Ollama
inference request, zero compaction calls, and no new effect on same-key replay. The final normalized
request carried about 4.1k measured Qwen input tokens and a 2,048-token output limit, fitting the 8,192
provider context gate. PostgreSQL stored the GGUF/Qwen tokenizer identity and Ollama renderer identity;
the digest on the consumed permit matched the exact normalized bytes received by the fake upstream.
The fake provider saw the Task Capsule and prompt schema, no tools or `response_format`, and no
`store` field. Its 37 input/4 output usage values are synthetic fake-provider observations and do not
verify Ollama usage semantics.

The successful runtime probe records six read-only Ollama metadata requests, Ollama 0.34.0, pinned
Letta App Server 0.33.8, SDK 0.8.25, Node 22.19.0, and bridge protocol 1. The GGUF tokenizer fixture
contains 8 CPU-reference token vectors and 5 Go-renderer prompt vectors. The targeted Qwen tests compare
their token IDs, rendered bytes, settings snapshot, profile drift, and request ceilings. The generated
bridge schema was regenerated twice with identical output; bridge build, Python compile, lock check,
and whitespace check passed. The Phase 6B `LEGACY_UNMEASURED` settlement regression passed in a fresh
PostgreSQL 16.15 container; its run record is
`integration/runtime/artifacts/p6b-legacy-c469d211.json`.

The failed diagnostic runs remain as separate timestamped artifacts. The run that rejected the pinned
SDK's `store=false` request left its isolated database's Task `RUNNING`; read-only inspection found no
provider-call or permit row and zero held/spent budget. That database and row were not repaired or
reused. A later probe assertion failure was only a decimal text-format comparison; its isolated Task
and call had completed and settled at zero tariff. The passing probe used a fresh database.

No real Qwen model was loaded or called. Effective Ollama context and Qwen usage interpretation remain
unverified. Before the one-call local smoke, verify the effective 8,192-token context independently,
review the current read-only metadata, and use a fresh isolated database/scope. Keep mandatory gateway
permits and the production dispatch block in force; stop on an UNKNOWN execution rather than retrying
through another session.
G7 same-execution resume and G8 production exact-request validation remain separate follow-up work.
