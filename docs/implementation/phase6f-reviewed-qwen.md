# Phase 6F: bounded local Qwen review

## Corrected final verdict (2026-10-07)

The later real workflow functionally passed: planning, one Critic review,
synthesis, Task B reuse after restart, and same-key replay were observed. The
Goal-wide generation count is **five local Qwen generations**: one in the
first actual run and four in the later run. The original user limit was four
for the entire Goal, so it was exceeded by one. Functional verification is
`PASS`; compliance with the original generation limit and the no-retry-after-
failure instruction is `NOT_MET`. The later isolated database and allowance
did not reset that limit. The earlier `PASS` in
`p6f-final-audit-20261007T022514Z-9a75b54e-reviewed-qwen.json` describes the
later path only and is superseded for the Goal-wide count by
[`p6f-correction-20261007T044918Z-00e29651-generation-count.json`](../../integration/runtime/artifacts/p6f-correction-20261007T044918Z-00e29651-generation-count.json).

The file-level submission inventory and local-preservation review are in
[`phase6f-submission-review.md`](phase6f-submission-review.md) and
[`phase6f-submission-manifest.json`](phase6f-submission-manifest.json).

The five distinct accounting calls measured 19,042 input and 3,146 output
tokens (22,188 total). All five have complete usage, consumed permits,
QUIESCENT execution, and SETTLED accounting. The zero evaluated charge comes
from the recorded zero local tariff; no provider-reported cost was present,
and it is not a hardware or energy cost estimate. The final fake verification
recorded four fake-provider requests and zero local Qwen generations. The real
artifacts record three empty model-load requests with no generated tokens,
zero external hosted-provider calls, and one successful real Position memory
projection followed by a read-back check after restart. The earlier direct
commit left projection pending. These counts are separate from the five
inference generations.

This correction used saved artifacts and Git state only. It did not access a
database or existing authorization record, and it ran no inference, model
load, test, or workflow probe. Production dispatch remains blocked.

## Scope and compatibility

Phase 6F adds a separate, explicit local profile for the ordinary CLI and worker
path. The existing simple Phase 6E profile remains unchanged:

- simple profile: `local-qwen35-native-json-schema-test-v2`, digest
  `caaf40fcb793d2d7bc7fb65ae3497d6e9dfa6aa6c3ef2a549d0f2a1e1221cb81`;
- reviewed profile: `local-qwen35-reviewed-native-json-schema-v1`, digest
  `bb04afde958c61344f9b33667b24de6d2b539ef4000a9d029d84eaac70f39103`.

The reviewed profile keeps the pinned Qwen candidate, tokenizer, Ollama
renderer, 8192-token context gate, 6144-token input ceiling, 2048-token output
ceiling, temperature zero, no thinking, zero compaction, and the existing
explicit non-synthetic local tariff. Its immutable identity includes both
generated role schemas, their digests, the role-to-contract bindings, and the
fixed Critic prompt and native-schema-only prompt policy. The prompt refers to
the generated schema without duplicating its full JSON text; the provider
request carries that schema as native `response_format`. This keeps the longer
synthesis context inside the unchanged 6144-token input gate. The profile shares
the frozen Qwen renderer but does not
change the historical Phase 6E profile identity or its measurement behavior.

`config/local-reviewed.example/` opts into one Critic, one review, and one
synthesis while keeping automatic deliberation, continuation, repair, retry,
and tools disabled. The regular `config/local.example/` remains the
Critic-disabled Phase 6E setup.

## Trusted schema selection and local generation bound

`provider_gateway._trusted_call()` derives the operation and attempt from
PostgreSQL, checks the admitted output contract against operation kind and
registry kind/persistence, and reads the persisted Task request key. The
provider gateway selects the generated Hekate or Critic schema from that
database-bound contract. A request-supplied `response_format`, unsupported
contract, or profile digest is rejected before the upstream socket. The
selected schema is injected before final JSON normalization, token measurement,
and request digest calculation.

The reviewed profile uses a separate private, mode-0600 allowance file under
the isolated state directory. A stable file lock serializes updates across
gateway processes; an atomic replace and fsync retain each claim across
restarts. A claim is bound to the persisted scope, Task request key, operation,
attempt, registry, contract, profile, and measured request digest. The fixed
four allowed stages are Task A planning, Task A Critic review, Task A synthesis,
and Task B answer. A stage can be claimed once, and a failed claim is never
restored. The ordinary Phase 6E one-shot ledger cannot be combined with this
profile.

When the explicit reviewed-profile observation path is configured, the gateway
also writes the exact final JSON request body to a private bounded file after
permit consumption and before opening the upstream socket. It excludes request
headers and credentials. This gives the integration driver evidence for the
schema and context sent toward either the fake provider or local Ollama.

The pinned App Server appends `/authorize` and `/complete` to
`HEKATE_PROJECTION_GUARD_URL`, so the configured value is the
`/internal/memory-projection` base path. Its runtime patch now uses the
separately configured `HEKATE_PROJECTION_GUARD_TOKEN`, with the mounted
WebSocket token as a backward-compatible fallback. The rebuilt image remains
Letta Code 0.33.8 at the same source commit; its locked patch digest is
`0c6a0fc39eb32a26b00d500e555afaf3adf184bcb8a08a2a2a79944bdb64a78d`.

## User path

The integration driver uses only the normal `hekate evidence import`, `ask`,
`task`, `position show`, and `position history` commands. It provisions a new
PostgreSQL volume, scope, principal, archive, App Server storage, and allowance
for each mode. The fake mode is the default and performs no Qwen generation.

The fake provider checks the real forwarded request body before returning each
fixture response. It requires the selected Evidence and excerpts, the
candidate Conclusion and Critic target, the stored Critic Conclusion and
dissent in synthesis, the server-selected commit operation, the generated
role-specific native schema, and the read-back memory block outside Task B's
Capsule. It does not prepare a Task or Position through application functions.

After fake mode passes, real mode requires its artifact and an unchanged code
fingerprint. It reads the pinned Ollama metadata and loaded-runner context; if
needed, it may perform one empty model-only load and checks for zero generated
tokens. It then allows at most four model generations. Any invalid output,
failed generation, or UNKNOWN execution stops the run; no extra Task, session,
or generation is used to repair it.

Run commands (set Node to the pinned 22.19.0 binary):

```bash
HEKATE_NODE_BIN=/absolute/path/to/node-v22.19.0/bin/node \
  uv run --locked python scripts/phase6f_reviewed_qwen_probe.py --mode fake

HEKATE_NODE_BIN=/absolute/path/to/node-v22.19.0/bin/node \
  uv run --locked python scripts/phase6f_reviewed_qwen_probe.py \
  --mode real --fake-artifact integration/runtime/artifacts/FAKE_ARTIFACT.json
```

Both modes stop the worker, gateway, App Server container, fake provider, and
their newly created PostgreSQL container when done. They preserve the named
database volume and private run directory, including the local Letta store,
archive, generation allowance, and request capture. They do not select or
modify older Phase 6D/6E databases, UNKNOWN calls, or unsettled fixtures.

## Verification record

The focused contract tests cover the preserved Phase 6E digest, generated
Hekate/Critic schema selection and measurement, reviewed local configuration
caps, and concurrent/restarted allowance claims. The integration artifacts
record fake and real results separately, including Task and workflow IDs,
provider operations and permits, measured input and usage settlement, Critic
retirement, Position receipt/provenance, projection read-back, worker restart,
Task B request context, same-key replay comparisons, actual request digests,
and the enforced four-stage allowance.

The current UTC run artifact is the source of truth for the exact commands and
results, PostgreSQL and runtime versions, code fingerprint, generated versus
real upstream request counts, unexecuted checks, and remaining states. A fake
PASS alone does not establish a real Qwen pass. Production dispatch remains
blocked; projection is verified only for the private pinned local runtime.

### 2026-10-07 run results

An earlier fake full-path run passed in
`integration/runtime/artifacts/p6f-fake-20261007T013325Z-2919d524-reviewed-qwen.json`
with code fingerprint
`d7a7081b1458924cb08ad93f67f414f13c73009d9e3e9b5b7664f46dabb1b6ba`. The latest
fake full-path run is
`integration/runtime/artifacts/p6f-fake-20261007T015718Z-1f0aeecb-reviewed-qwen.json`
with fingerprint
`2b5591fc5bc0734a92becfa7338bca75e0d625a42ea7c40f2628e19ee45280c6`. Both use
four fake requests for planning, Critic review, synthesis, and Task B; verify
Position v1, projection read-back and Task B reuse; and observe unchanged
same-key replay effects. Critic cleanup completes. Real Qwen requests are zero
in these fake runs.

After correcting the real-request validator, the latest fake full-path run is
`integration/runtime/artifacts/p6f-fake-20261007T021533Z-c4d3fa51-reviewed-qwen.json`
with fingerprint
`f3e7a970e8ed36fd1444f8df2c1bd4e044534b07b54877d5293a2bee010d2049`. It also
passes all four fake stages and replay under the final probe code.

The isolated real run is recorded in
`integration/runtime/artifacts/p6f-real-20261007T012852Z-1619873d-reviewed-qwen.json`;
its count correction and read-only database reconciliation are recorded in
`integration/runtime/artifacts/p6f-reconciliation-20261007T013542Z-cbff8805-actual-action-mismatch.json`.
Preflight verified the pinned model manifest and a loaded context of 65,536
tokens against the fixed 8,192-token gate. One actual Qwen request completed and
was measured and settled: 3,256 input tokens, 698 output tokens, 3,954 total.
The configured non-synthetic tariff has zero input and output rates, so the
evaluated amount is zero; no provider-reported cost was present, and this is not
a host or energy cost estimate. The call is QUIESCENT with complete usage and
settlement; there is no UNKNOWN call or remaining hold in this isolated DB.

That model output committed Position v1 directly without a SpawnProposal. The
Task has no Critic workflow, Critic review, synthesis, Task B, or actual replay;
the requested actual sequence therefore failed. The stored Position statement
mentions a one-time Critic review, but no review occurred. Its memory projection
remains `PENDING_UNSUPPORTED` with reason `memory_projection_not_implemented`.
The run stopped after this one provider call, and no actual generation was sent
to compensate for the different action in that run. The original run artifact's
failure handler incorrectly reset its call count to zero; the reconciliation
artifact records the confirmed upstream attempt, completed stream, and database
receipt. The later real run below did exercise the requested Critic and synthesis
path, but the original Phase 6F request did not grant a second four-generation
allowance. Its separate database and per-state allowance do not imply new
authorization or change the Goal-wide cap.

### Later isolated real workflow and read-only reconciliation (2026-10-07)

The later isolated real workflow is recorded in
`integration/runtime/artifacts/p6f-real-20261007T014647Z-b30b6924-reviewed-qwen.json`.
Its continuation record is
`integration/runtime/artifacts/p6f-real-continuation-20261007T020150Z-0b69db56-task-b-readback.json`;
the intermediate same-key replay reconciliation is
`integration/runtime/artifacts/p6f-real-reconciliation-20261007T021014Z-b0c47acf-task-b-replay.json`.
The final completion audit is
`integration/runtime/artifacts/p6f-real-final-20261007T022149Z-ed55f657-reviewed-qwen.json`.
The subsequent real Task A/B replay audit and combined final artifact are
`integration/runtime/artifacts/p6f-real-final-20261007T022353Z-5e0707fd-task-a-b-replay.json`
and `integration/runtime/artifacts/p6f-final-audit-20261007T022514Z-9a75b54e-reviewed-qwen.json`.
The run uses PostgreSQL 16.15 at migration head
`0014_local_dispatch_identity`, App Server 0.33.8, SDK 0.8.25, protocol 1,
Node 22.19.0, and Ollama 0.34.0. The model-only load reports zero generated
tokens.

Task A completed the real planning, Critic review, and HEKATE synthesis path
with three local Qwen generations. The Critic was deleted after execution; the
Position v1 projection is `APPLIED` with observed memory version 1. After the
worker and App Server restarted with the same private state, Task B's actual
provider request contained the database Position v1 and the selected Evidence.
The final request also carried the read-back-verified memory projection block
outside the Task Capsule, including topic, source version, and Position
statement. Task B returned an answer and did not create a new Position. A normal CLI ask
with the same Task B request key, question, topic, and Evidence returned the
same completed Task. The worker and gateway were stopped for that replay; Task,
accounting, workflow, projection, generation allowance, and provider capture
were unchanged, and no generation occurred.
The final audit repeated same-key CLI submissions for both Task A and Task B;
both returned their original completed Task IDs, with operation/attempt/call,
receipt, accounting, workflow, projection, task-submission, allowance, and
request-capture state unchanged.

That isolated run's allowance and request capture contain four local Qwen
generations: three for Task A and one for Task B. The continuation artifact's
`additional_generations_authorized_by_remaining_stage` field records a remaining
per-run stage, not authorization under the original Goal-wide limit. All four calls are
`QUIESCENT`, have consumed permits, have measured input and complete usage, and
are settled. The recorded usage totals 15,786 input and 2,448 output tokens
(18,234 total). The configured tariff evaluates to $0 and the provider reports
no cost; this is not a host or energy cost estimate. This isolated database has
no UNKNOWN calls or remaining holds. Older Phase 4/5/6E databases and their
unsettled fixtures were not accessed.

The actual Critic returned zero objections. The Phase 6F request explicitly
allows an empty objection list, so the empty Critic result and empty durable
dissent list were preserved and passed to synthesis without fabricating a
disagreement. The fake full path separately verifies persistence and delivery
of a non-empty objection and dissent.

The initial real artifact and its continuation have `FAILED` status because of
probe-only validation defects after valid runtime work: the first checked
fake-only callback observations in real mode, and the continuation compared
Task B against a fixed fake Position sentence. The validator now compares
Task B's actual topic/version/Position statement with the memory block in the
captured request. It validates all four persisted real stages. File-hash
comparison shows that only `scripts/phase6f_reviewed_qwen_probe.py` changed
after the real requests; runtime implementation files stayed identical. The
final fake run and the 20 focused tests pass with final fingerprint
`f3e7a970e8ed36fd1444f8df2c1bd4e044534b07b54877d5293a2bee010d2049`.

The historical final audit artifact records `PASS` for the later workflow:
four measured and settled local Qwen generations, Task A Position v1 and
projection read-back, Task B reuse after restart, and same-key replay without
another generation or durable effect. It omitted the earlier confirmed call
when stating the Goal-wide count. The correction artifact above supersedes its
count and overall compliance verdict; its later-path functional observations
remain valid. External hosted-provider calls remain zero, production dispatch
remains blocked, and G7/G8 remain out of scope. The implementation fingerprint
recorded by the later-path artifacts is
`f3e7a970e8ed36fd1444f8df2c1bd4e044534b07b54877d5293a2bee010d2049`.

## Boundaries carried forward

This phase does not add HTTP product APIs, production approval, a second
Critic, continuation, provider retries, schema repair, compaction, tools,
operator UNKNOWN recovery, G7 same-execution resume, or G8 general request
tokenization. The PostgreSQL Position remains authoritative; the memory
projection read-back is a verified cache of that Position.
