# Phase 6F submission review

Reviewed 2026-10-07 from saved artifacts and the current worktree only. The
file-level byte counts and SHA-256 values are in
[`phase6f-submission-manifest.json`](phase6f-submission-manifest.json). The
manifest lists every changed tracked implementation/config/test file, the
selected configuration files, the correction report, and the evidence needed
to review the claimed path. It excludes itself from its own hash list.

## Corrected verdict

- The later real workflow functionally passed: Task A planning, one Critic
  review, synthesis, Position v1, Task B reuse after restart, and no-effect
  same-key replay.
- The entire Phase 6F Goal used **five actual local Qwen generations**: one in
  the first run and four in the later run. The original Goal-wide limit was
  four, so it was exceeded by one. Whole-Goal compliance is **NOT_MET** even
  though the later workflow's functional result is **PASS**.
- The original first-run artifact reports zero generations, but its paired
  read-only reconciliation confirms one completed, measured, settled call.
  The later run's separate database and allowance do not reset the original
  limit. The prior combined PASS report is superseded for its whole-Goal count
by [`p6f-correction-20261007T044918Z-00e29651-generation-count.json`](../../integration/runtime/artifacts/p6f-correction-20261007T044918Z-00e29651-generation-count.json).
- The five unique calls total 19,042 input plus 3,146 output tokens. All five
  are QUIESCENT, usage-complete, and SETTLED. Zero evaluated tariff is not
  provider-reported cost or a hardware/energy estimate.
- The final fake proof contains four fake requests and zero local Qwen
  generations. The real Critic returned zero objections, which was permitted
  and preserved; the fake flow separately verified a non-empty dissent. The
  saved real artifacts report three empty model-load requests with no
  generated tokens, zero external hosted-provider calls, and one successful
  Position memory projection plus a read-back check after restart.

## Submitted files

The manifest is the authoritative inventory. The selected set contains:

- Every changed tracked file, including the provider-call patch and its matching
  `integration/letta/versions.lock.json` entry.
- The five files under `config/local-reviewed.example/`, the reviewed profile,
  the Phase 6F probe, and its focused tests.
- The Phase 6F implementation note, local quickstart, and this review.
- The first-run raw artifact and its correction; the blocked zero-call attempt;
  the later real workflow, Task B continuation, historical audits and replay;
  the final fake proof; and the new count correction.

The artifact entries label historical statuses and explain which PASS values
apply only to the later path. The original user request text, private runtime
state, database data, and raw request captures are not submission candidates.

## Preserve locally

Do not move or delete the following. They are outside the submission set and
were not read or hashed during this review:

- `/home/hekate/.local/share/hekate/phase6f-reviewed-qwen/p6f-real-20261007T012852Z-1619873d/` and `/home/hekate/.local/share/hekate/phase6f-reviewed-qwen/p6f-real-20261007T012852Z-1619873d/real/state/`.
- `/home/hekate/.local/share/hekate/phase6f-reviewed-qwen/p6f-real-20261007T014647Z-b30b6924/` and `/home/hekate/.local/share/hekate/phase6f-reviewed-qwen/p6f-real-20261007T014647Z-b30b6924/real/state/`.
- `/home/hekate/.local/share/hekate/phase6f-reviewed-qwen/p6f-fake-20261007T021533Z-c4d3fa51/` and `/home/hekate/.local/share/hekate/phase6f-reviewed-qwen/p6f-fake-20261007T021533Z-c4d3fa51/fake/state/`.
- Private PostgreSQL volumes `hekate-p6f-1619873d`, `hekate-p6f-b30b6924`, and `hekate-p6f-c4d3fa51`.
- Intermediate fake-run artifacts and superseded diagnostic reconciliations listed in the manifest; the final fake artifact and later audits are the submission evidence.
- Local Qwen model weights, credentials, private environment files, caches, build outputs, and node/virtualenv directories.

The private directories contain runtime state such as the private allowance,
local Letta store, archive, and request captures. The correction did not open
these files or query the preserved databases. Existing private state remains
untouched.

## Review checks

The correction parsed the listed source JSON artifacts, reconciled unique calls
by immutable `accounting_call_id` and cross-checked operation, attempt, Task,
provider-call, usage, and settlement identities. It verified that the runtime
patch SHA-256 equals the lock entry. Selected text-file secret scanning,
manifest JSON validation, `git diff --check`, Markdown/JSON trailing-whitespace
review, and staged-state confirmation are recorded in the correction artifact
and manifest. No test, probe, model load,
inference, database access, `git add`, commit, push, PR, or merge was performed
for this correction.
