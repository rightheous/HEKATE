# Phase 6D: one local Qwen task attempt

## Result

The pinned local model was loaded once without generation, and its live Ollama runner reported a
65,536-token context. One Task then reached the pinned Letta runtime, the Qwen request measurer, and
the PostgreSQL permit path. It did not produce an Ollama chat request or a user response. The
pre-socket probe guard rejected the call after the permit had been consumed, and the runtime result
remains `UNKNOWN`. No retry, resume, or replacement Task was sent.

The run artifact is
`integration/runtime/artifacts/p6d-20261006T042104Z-b6afec32-ollama-qwen-runtime.json`. A later
fake-provider run after correcting the guard is recorded in
`integration/runtime/artifacts/p6d-20261006T042733Z-e831c9f2-fake-preflight-guard-fix.json`; the
current reconciliation record is the timestamped `p6d-*-reconciliation.json` artifact beside them.

## What happened

The one-shot model load used Ollama 0.34.0's empty-prompt `/api/generate` load path. Ollama returned
`done_reason=load`, an empty response, and no prompt or evaluation counts. `/api/ps` then showed the
pinned Qwen model loaded with a 65,536-token context. This was a model load, not a generated answer.

The Task request was measured at 4,088 input tokens with a 2,048-token output ceiling. The gateway
authorized the request and PostgreSQL consumed one permit. The local opener guard compared the
authorization profile digest with the candidate manifest's profile digest, although the operation
was bound to the execution profile digest. That mismatch stopped the request before the underlying
HTTP socket opened. The gateway's internal opener counter is one; durable upstream attempts,
provider response observations, and confirmed Ollama inference calls are all zero.

The probe guard now receives the bound `execution_profile.content_digest`. A focused check accepted
that digest and rejected the distinct candidate digest, and the pinned fake-provider probe passed
after the correction. That validates the guard and fake runtime path; it does not rerun the local
Task. Because the original runtime operation is durably `UNKNOWN`, the one-shot policy forbids
retrying it or starting another Task to bypass that state.

## Preserved database state

The isolated PostgreSQL 16.15 database is at migration head
`0013_provider_token_measurement`. Read-only inspection found the Task still `RUNNING` past its
deadline, with no task response. Its `hekate.turn` operation, attempt, and provider call remain
`UNKNOWN`/`RUNNING`; the provider call is `MEASURED`, its permit is `CONSUMED`, and its usage
projection is `UNKNOWN` with settlement `PENDING`. The operation-envelope reservation is also
`PENDING_SETTLEMENT` (amount zero); system and Task budget rows show zero spent and held. Those zero
balances do not establish settlement. No provider usage, terminal execution proof, or cost was
invented to normalize the state.

The database container and this state were left available for inspection. The persistent HEKATE
execution cannot be declared successful from the fake-provider result.

## Scope and next boundary

The corrected fake-provider runtime completed a separate synthetic Task with one fake request and
one measured, consumed permit. The focused Qwen unit tests passed 5/5 before the final probe-guard
edit; Qwen tokenizer and renderer code was not changed by that edit. The final probe script was
compiled and whitespace-checked after the edit. No real Ollama inference request was made, so there
is no generated answer, provider-reported token usage, or settled real-call accounting to report.

Production dispatch remains blocked. G7 same-execution resume and G8 production request/usage
validation remain unresolved. The existing Phase 6C profile stays test-only. At the time of the
first run, no additional local attempt was authorized; the later independent authorization is
recorded separately below and does not permit resuming the original UNKNOWN execution.

## Separately authorized independent attempt

The follow-up user instruction explicitly permits one new independent test while retaining the
original Task, operation, consumed permit, and pending settlement as-is. The original operation is
not retried or resumed. Its PostgreSQL container was absent when this authorization was rechecked;
the last read-only state snapshot remains in the original run and reconciliation artifacts, and no
replacement rows were fabricated.

The probe now requires `--authorized-independent-attempt-after-unknown` to select the separate,
fixed `p6d-qwen-independent-authorized-attempt-ledger.json`. Before selecting it, the probe verifies
that the original ledger and artifact refer to the same run and show zero upstream attempts and no
provider response. The new ledger is created with exclusive file creation and cannot be reset by a
new run ID. Its authorization record points back to the prior run and artifact hash. The previous
ledger is not rewritten.

The local pre-socket guard now validates the exact `urllib.request.Request.data` digest against the
execution profile, provider authorization, consumed permit ID/call ID, and the consumed permit's own
request/profile digests. The candidate profile digest remains a separate model identity. The focused
test exercises the actual one-shot opener with a local counting transport: the matching request may
reach the transport boundary, while request, permit, or candidate-digest substitutions are rejected
before it. No network or provider call is made by that test. Same-key replay now submits the original
question bytes, so it exercises the existing receipt path instead of changing the request under the
same key.

## Authorized independent attempt result

The separately authorized independent run used a new PostgreSQL 16.15 database and scope. It left the
original UNKNOWN Task untouched. The preflight fake-runtime artifact is
`integration/runtime/artifacts/p6d-20261006T150013Z-8a559c66-ollama-qwen-runtime.json` and passed
with one fake request, zero Ollama inference requests, and zero upstream generation attempts.

The actual local run is recorded in
`integration/runtime/artifacts/p6d-20261006T150156Z-7f915cee-ollama-qwen-runtime.json`; the
read-only outcome correction and database snapshot are in the adjacent
`integration/runtime/artifacts/p6d-20261006T150950Z-ffa98a36-independent-attempt-diagnostic.json`
artifact. The pinned App Server
0.33.8, SDK 0.8.25, bridge protocol 1, and Node 22.19.0 were used. Ollama 0.34.0 served the pinned
`orcarouter/Qwen3.8-27B-Uncensored:iq4_xs` model. A no-generation model-only load ran once and returned
`done_reason=load`; the loaded runner reported a 65,536-token context, above the unchanged 8,192-token
gateway limit. Input and output caps remained 6,144 and 2,048 tokens.

The correct execution-profile and request digests passed the pre-socket guard, matched the consumed
PostgreSQL permit, and matched the final outgoing request bytes. Exactly one upstream generation
attempt reached Ollama. Ollama returned HTTP 200, an SSE completion marker, `finish_reason=stop`, and
provider usage of 4,095 input, 394 output, and 4,489 total tokens. The input usage matched the
gateway measurement and the database record. PostgreSQL recorded the call as QUIESCENT, the permit
as CONSUMED, usage as COMPLETE, and settlement as SETTLED. The configured external tariff evaluated
to $0; this is a pricing policy result, not a claim that the run had no physical or energy cost.

The Task did not succeed. Qwen began a fenced JSON object containing the correct arithmetic answer,
but its final output stopped while emitting the confidence object's `missing_evidence` key. The
pinned bridge rejected the incomplete output under the strict schema contract. The Task and attempt
therefore ended FAILED with POLICY / `output_not_valid`; the only user-channel response is the
server-generated failure message.
The model text was not repaired or accepted. Same-key replay returned the same Task and added no
upstream request or accounting effect. The independent one-attempt allowance is consumed; no retry,
second Task, or additional local inference was made.

The original run artifact's `unexecuted` field retained a note captured before Task admission. The
probe was corrected afterward to store that observation as `pre_request_gate` and to write final
unexecuted boundaries after the actual attempt count is known. This reporting-only correction did
not change the provider request path. The focused offline guard/report tests passed 2/2, and the
diagnostic artifact records both the execution fingerprint and the final code fingerprint.

This run verifies the real request, measurement, permit, provider usage, execution termination,
settlement, and replay boundaries, but it does not meet the Phase 6D successful-answer completion
condition. Any further successful local execution requires a newly authorized attempt and a verified
output-format correction. The original UNKNOWN execution remains separate and unchanged. Production
dispatch is still blocked; G7, G8, and Letta memory projection remain deferred.

## Native JSON Schema follow-up

The separately authorized follow-up test uses a separate `qwen35_native_json_schema_test_v2` execution profile and a
separate exclusive one-attempt ledger. It does not reuse either earlier local-attempt ledger, Task,
database, scope, registry, session, or runtime operation. The earlier FAILED/POLICY run and UNKNOWN
execution remain read-only and are never resumed.

The provider gateway selects `hekate-turn-output.v1.schema.json` from the Python contract exporter
only after it has loaded the admitted operation's `output_contract`. It adds Ollama's native
`response_format: {type: "json_schema", json_schema: {schema: ...}}` before request normalization,
prompt measurement, digest binding, permit issuance, and the final pre-socket byte comparison. The
schema remains embedded in the prompt and checked by the pinned bridge and server. SDK
`outputFormat` stays disabled and tools stay empty. Temperature is fixed to zero as a request-policy
setting; it is not treated as a guarantee of valid output. The schema is still part of the measured
prompt text; internal grammar-control tokens are not separately measured or claimed.

Ollama v0.34.0 declares `response_format` as a JSON Schema input and passes the schema through its
OpenAI adapter into the native `Format` field. The [pinned OpenAI adapter](https://github.com/ollama/ollama/blob/v0.34.0/openai/openai.go),
[pinned server route](https://github.com/ollama/ollama/blob/v0.34.0/server/routes.go), and
[structured-output API documentation](https://docs.ollama.com/capabilities/structured-outputs)
describe that path. No separate CPU schema-to-grammar converter is installed in this environment,
so the local probe records that offline grammar compilation was unavailable; the fake-provider
check does not claim to compile Ollama's grammar.

The authorized attempt ledger is created with exclusive file creation before any optional
model-only load. A current `/api/ps` snapshot is used first: the zero-generation load runs at most
once and only if the pinned model is not already loaded. A second `/api/ps` observation immediately
before the chat socket open checks the exact model digest and an 8,192-token minimum context. The
ledger records zero or one model-only load and can never be reset by changing the run ID.

For the public arithmetic test response, the gateway's bounded SSE observer records content-delta
count, UTF-8 byte count, and digest per choice, plus finish reason and `[DONE]`. Reasoning-like
delta fields, if emitted, receive separate counts and digests without retaining their text. The
accepted Task response is compared with the assistant-content digest and byte count; no raw provider
response is added to the artifact except the accepted public answer.

The follow-up runtime outcome and its database, request, usage, settlement, replay, and one-attempt
ledger observations are recorded in a new timestamped `p6d-*-ollama-qwen-runtime.json` artifact.
The artifact is the execution evidence; this section describes the stable request and observation
policy.

### Authorized attempt result

The separate native-schema attempt is recorded in
`integration/runtime/artifacts/p6d-20261006T162211Z-18c1dad6-ollama-qwen-runtime.json`. It used a
fresh PostgreSQL database and scope, a new persistent HEKATE registry/session, one empty-prompt
model-only load, and a new exclusive one-attempt ledger. `/api/ps` reported the pinned digest and a
65,536-token loaded context before the chat request; the provider gate remained 8,192 tokens. No
second model generation was sent.

The actual request carried the Python-generated schema SHA-256
`571098e90cb3597d768f15ce4c89f9b57ee328a39a747b0597657018fe9be54a` in native
`response_format`, with temperature zero and no tools. The final request digest matched both the
measured request and consumed PostgreSQL permit. Ollama returned HTTP 200, `finish_reason=stop`,
and `[DONE]`. Input usage was 4,078 tokens, output usage 333, total 4,411; provider input usage
matched the measured input and PostgreSQL. The call became QUIESCENT, its permit stayed CONSUMED,
usage was COMPLETE/SETTLED, and the configured external tariff evaluated to $0. Host and energy
costs remain unmeasured.

The Task did not succeed. The bridge stored 595 bytes of raw output; read-only validation found an
incomplete JSON document at the end and no admissible Hekate proposal. The server returned its
58-byte POLICY failure response, and the Task ended FAILED / `output_not_valid`. No raw generated
text was added to the artifacts.

The gateway accumulated 760 UTF-8 bytes across 265 assistant-content delta events, while the pinned
bridge recorded 595 bytes with a different SHA-256. `finish_reason=stop` and `[DONE]` were observed;
no separate reasoning delta field was reported. This proves a content-path discrepancy for this
attempt, but the exact provider text was not retained, so the lost/differently classified segment
cannot be attributed conclusively. The bridge/SDK source review found no confirmed local
accumulation or terminal-order defect; no code change was made based on an unproven cause. Same-key
replay returned the same Task without another upstream request or accounting effect. The follow-up
artifact preserves the single failed attempt; further inference requires a new explicit user
authorization.

### Provider-to-database output-delivery diagnostic

The earlier failed run is preserved at
`integration/runtime/artifacts/p6d-20261006T162211Z-18c1dad6-ollama-qwen-runtime.json`.
Its provider choice 0 and rejected database result share the same Task, operation, attempt,
registry, and accounting-call identity. The observed endpoints differ: provider content was 760
UTF-8 bytes over 265 deltas (`1ef0f1a8…e81bef`), while bridge `raw_output` was 595 bytes
(`34d2819a…ad12e`). The generated provider text was not retained. App Server and SDK hashes were
not captured for that attempt, so the interval is known but the first divergent component and root
cause are not.

The new fake-only reproduction is
`integration/runtime/artifacts/p6d-20261006T184411Z-e45f71c9-output-delivery.json`. It used
PostgreSQL 16.15 at migration `0013_provider_token_measurement`, Letta App Server 0.33.8, SDK
0.8.25, bridge protocol 1, and Node 22.19.0. The complete synthetic response matches the historical
stream's observed 760-byte, 265-content-delta, separate-`finish_reason` shape and `[DONE]`; it also
uses mixed LF/CRLF and HTTP chunk boundaries inside UTF-8 code points. A separate synthetic
reasoning event is hashed independently. This fixture is newly generated test content, not a
reconstruction of Qwen's missing response.

For the accepted complete response, provider choice 0, gateway-reassembled content, pinned App
Server assistant events, SDK assistant messages and result, bridge `raw_output`, and PostgreSQL
`raw_output` all matched at 760 bytes and SHA-256
`8bbc52ac24f13a60857da055c7911a5c3f65998932595eb2bea2fa29f212d652`. The output was accepted by
the strict schema and the Task completed. Removing only the final `}` produced 759 bytes; each
observed stage preserved those bytes and the server rejected the result with FAILED / POLICY and no
Conclusion. After worker restart, same-key replay left the two fake requests and accounting effects
unchanged. Total fake-provider requests were two; Ollama/provider inference calls were zero.

The historical output loss did not reproduce in this pinned end-to-end fixture, including with the
same observed content size, delta count, and terminal-frame arrangement. No provider, App Server,
SDK, or bridge accumulation fix is justified by this run. The code change adds opt-in digest-only
stream observations for the diagnostic and fixes the probe's multi-request result check; it does not
retain generated provider text or relax output validation. The adapter's internal `text_delta` was
source-reviewed but not directly instrumented. A future diagnosis of the historical incident needs
intermediate observations on an authorized real response or preserved raw fixture content; this
turn made no real generation request.

The existing actual Qwen Task remains FAILED / POLICY with its original settled usage record. Its
state was not retried or modified. Production dispatch remains blocked; G7/G8 and Letta memory
projection remain deferred.

### One-call Qwen output observation and captured replay

The single separately authorized Qwen generation is recorded in
`integration/runtime/artifacts/p6d-20261006T190724Z-ab1a653c-output-observed-qwen.json`. It used a
fresh Task (`dfe17e20-b154-45c5-8468-f83c5ef61f8e`), operation
`2c4320bb-d3e4-51ed-a8d7-7b7aa89078c9`, accounting call
`3baeadc7-373e-4e7f-a4cc-57649f8e785e`, permit
`f6012d89-6e24-515b-9a3c-2d02cdfc101d`, and persistent HEKATE registry
`6d6949f6-9498-515e-9677-a4056ca98689`. The request used the frozen native-schema execution
profile (`caaf40fc…1221cb81`), output schema
`571098e9…8be9be54a`, request digest `c472ba67…a3804717`, temperature 0, no tools, no
thinking, and no compaction. The provider reported the fixed Qwen model and loaded 65,536-token
context; the configured gateway context limit remained 8,192.

The bounded provider capture is in
`integration/runtime/artifacts/p6d-20261006T190724Z-ab1a653c-output-observed-qwen-capture/`.
The exact 64,872-byte SSE stream has SHA-256
`a8ce0a5c7ec82f3fd1eadc2c33b64b8fbe5b234b32ecf5ec87595b36f20b7829`; choice 0 contains 763
bytes, 270 assistant deltas, and SHA-256
`652fa52fb4023a574f3fe25488c2632c4c3d90aa70f1d438178ecb83075e1563`. It ends with
`finish_reason=stop` and `[DONE]`. The captured content strictly parses as `HekateTurnOutput`, and
its Task, attempt, and registry claims match the original trusted binding. It contains the answer
“17과 25의 합은 42입니다.” There were no reasoning-channel deltas. The provider, gateway, adapter
input, and emitted `text_delta` agree per delta; the App Server event observer also recorded all
270 matching deltas (763 bytes). The original run did not retain a combined App Server raw-text
file, so that boundary is verified by ordered per-delta hashes and byte counts rather than a
reconstructed aggregate hash.

The SDK recorded 240 assistant messages / 669 bytes before its 30,000 ms turn timeout. Its final
raw text and PostgreSQL `raw_output` are the same exact 669-byte provider prefix
(`4256735f…cec2af9ce`); no final SDK result text was produced. The App Server observer continued
after the SDK result and recorded the remaining assistant deltas followed by `usage_statistics` and
`stop_reason`. A read-only rerun of `validate_capsule_binding()` on the complete capture also
passed against the original Task, attempt, RegistryId, revision, and recorded runtime binding. The
Task therefore ended FAILED / POLICY; the answer was not accepted or exposed as
the user response. Read-only strict parsing of the full provider capture confirms the complete
model response itself is valid. The first divergence is the SDK stream consumer timeout, not
provider truncation, adapter channel classification, bridge accumulation, or PostgreSQL storage.

The request used one physical generation. Provider usage was 4,091 input / 338 output / 4,429
total tokens; input matched the exact request measurement. PostgreSQL records the call QUIESCENT,
permit CONSUMED, usage COMPLETE, and settlement SETTLED. The configured external tariff evaluates
to $0; host and energy costs are not measured. Same-key replay did not add a request or accounting
effect. No real provider call was made during the subsequent fake replays.

The probe now accepts an explicit 1–240,000 ms bridge turn timeout, and the bridge default is
240,000 ms, bounded by the existing Task deadline. This change was checked through the pinned fake
runtime, not through another Qwen call. A captured-response replay with the original 30,000 ms
timeout is recorded in
`integration/runtime/artifacts/p6d-20261006T192911Z-57507dd1-captured-sse-30000ms.json`; it
reproduced the SDK-first divergence and persisted a 493-byte prefix in its new isolated Task. The
replay applied an estimated 9.078-second request-to-first-App-Server-delta delay before fake HTTP
headers and replayed 22.899 seconds of later intervals. This is approximate: original upstream
header timing and HTTP chunk boundaries were not captured. The fake server recorded HTTP 200 and
completed the full 64,872-byte SSE body.

The 240,000 ms replay is recorded in
`integration/runtime/artifacts/p6d-20261006T193011Z-57b83bb2-captured-sse-240000ms.json`. All
observed assistant stages, SDK result, bridge output, and PostgreSQL output matched at 763 bytes
and the provider SHA-256. The response was rejected because the untouched captured Conclusion is
bound to the original Task, attempt, and registry; the replay deliberately used a different fresh
binding. The result is FAILED / POLICY, not evidence that the original Task can be retroactively
accepted. Same-key replay did not add a fake request or accounting effect. Replay usage is marked
synthetic; it does not represent another Qwen call or actual provider charge.

The 30-second replay also exposed a probe assertion that incorrectly required the local-Ollama
response observer in fake mode. The fake transport now records its own HTTP status, headers, bytes
sent, and completion state; the replay gate uses that observation and the durable call state. A
separate offline check corrected App Server attribution when only per-delta hashes were retained:
the report does not treat a missing aggregate raw hash as content loss. The multi-delta output
preflight passed again at
`integration/runtime/artifacts/p6d-20261006T192628Z-27d0c82c-output-delivery.json` (two fake
requests, zero actual provider calls).

Two earlier interrupted timing experiments remain untouched in their isolated databases and
artifacts (`p6d-20261006T192100Z-f8468a25` and
`p6d-20261006T192300Z-1fdc33bd`). Read-only inspection found each synthetic call still UNKNOWN,
with a consumed permit and usage UNKNOWN / PENDING. They were not normalized or settled. Another
early replay artifact (`p6d-20261006T192706Z-13da10c0`) records the assertion failure; its synthetic
call had already converged to QUIESCENT / COMPLETE / SETTLED. These states are separate from the
real Qwen call above.

The 19:07 Phase 6D run above did not achieve successful answer acceptance. A separately authorized
follow-up used a distinct database, Task, and single-use ledger; its result is recorded below.

### Fresh independent output-observation follow-up

The current user instruction authorized at most one new independent Qwen generation. The probe added
`--authorized-output-observation-followup`, which validates the consumed 19:07 ledger, its failed
Task artifact, the SDK-timeout diagnosis, and the bounded fake preflight before selecting the new
exclusive ledger `integration/runtime/artifacts/p6d-qwen-output-observed-followup-authorized-attempt-ledger.json`.
The earlier output-observation ledger and failed Task were left untouched. A first database URL
preflight stopped before migration because it used an unsupported driver scheme; correcting the
scheme reused the same still-empty isolated database and did not open the runtime or provider path.

The successful run is
`integration/runtime/artifacts/p6d-20261006T193948Z-8bc886ed-output-observed-qwen-followup.json`.
It used PostgreSQL 16.15 at `0013_provider_token_measurement`, Letta App Server 0.33.8, SDK 0.8.25,
protocol 1, Node 22.19.0, and Ollama 0.34.0. The exact pinned model digest matched the candidate;
the live runner was absent initially, so the probe performed one generation-free model-only load
and verified the loaded 65,536-token context before sending. The execution gate remained 8,192
tokens, with 6,144 input and 2,048 output token caps. The native schema hash was
`571098e9…8be9be54a`, temperature was zero, tools and thinking were disabled, and compaction was
zero.

This new Task (`c3161b4b-a517-4bdb-8047-8b61bb4f336d`) used operation
`bafb3dfb-eb72-5538-92b7-3934b294af23`, attempt
`d7018607-ee5b-5b60-9684-93be21722597`, accounting call
`54c60c34-d8a9-4f50-80d1-734883a20b8f`, permit
`f803d45c-7600-55fc-b10d-3f7a4b614deb`, and persistent HEKATE registry
`9df16e27-de37-5b74-be77-0f16456976de`. The provider response was `chatcmpl-239`. Qwen returned
“17과 25의 합은 42입니다.” The complete 763-byte choice 0 content was captured across 268
assistant deltas; its SHA-256 is
`5e140dd0e1c79754a8d0baddd34c10bba4a4b5d474f0736754a0a847f63fb5bb`. The 64,402-byte SSE
capture has SHA-256 `b75304b91316305af181b50126d39e6663546209d064dc5e466f1163707f579a`; it ended
with `finish_reason=stop` and `[DONE]`. No reasoning text was retained.

Provider choice 0, gateway content, adapter input, emitted `text_delta`, App Server assistant
stream, SDK messages/result, bridge output, and PostgreSQL `raw_output` all match at 763 bytes and
the same SHA-256. The bounded original response, ordered SSE frames, per-boundary event records,
and database output are retained under
`integration/runtime/artifacts/p6d-20261006T193948Z-8bc886ed-output-observed-qwen-followup-capture/`.
Strict JSON parsing, `HekateTurnOutput` validation, and trusted binding-to-permit matching passed;
there was no first divergence. PostgreSQL stored the exact user response and the Task reached
COMPLETED / COMPLETED.

The one physical call was measured at 4,086 input tokens, matching the provider usage; usage was
4,086 input / 336 output / 4,422 total. The call is QUIESCENT, the permit CONSUMED, usage COMPLETE,
and settlement SETTLED. The configured external tariff evaluates to $0; physical host and energy
costs remain unmeasured. The worker was stopped and restarted before replaying the same request
key. The replay returned the same Task and added no provider request, permit, ledger entry, or other
accounting effect. This run made one actual provider generation, zero fake-provider requests, and
no second generation.

The artifact records the execution code fingerprint and the final code fingerprint after this
documentation update. Production dispatch remains blocked. G7 and G8 remain deferred, and Letta
memory projection remains unexecuted. The earlier failed Task and synthetic UNKNOWN/settlement
fixtures were not used as this Task's state and were not normalized by this run.

Phase 6D successful answer acceptance is achieved for this fresh, separately authorized Task.
