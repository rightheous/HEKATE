# Phase 6E: general local CLI execution

## Scope

Phase 6E connects the ordinary CLI, explicit local configuration, PostgreSQL,
the existing worker and DB permits, the private gateway, and the pinned Letta
runtime for the frozen Qwen candidate. It does not approve production dispatch
and does not claim Qwen validation for Critic, Position commit, or memory
projection.

## Local command path

- `hekate init-local` applies migrations, creates or verifies the configured
  principal/scope/policy/epoch authorization row, and installs the private
  loopback gateway provider entry for Letta. Replay with identical config is
  idempotent; a different or inactive authorization row is not overwritten.
- `hekate doctor` performs read-only profile, asset, migration, authorization,
  Node/bridge, Ollama, gateway, and runtime checks. It distinguishes
  `PRESTART_OK` from `READY` and requests no inference.
- `hekate gateway` starts before the App Server and only binds to loopback.
- The normal `worker`, `ask`, `task`, and `cancel` commands use the existing
  application path. The CLI wait timeout does not change Task deadline or
  cancel an operation.

`HEKATE_CONFIG_DIR`, `HEKATE_PROJECT_DIR`, and configured state/archive paths
are independent. Local execution accepts only the frozen Qwen candidate,
8192 context, 6144 input, 2048 output, zero compaction, 900-second Task
deadline, no tools, and disabled Critic/continuation. The local external tariff
is explicitly non-synthetic and currently zero-valued; that is not a claim
about hardware or energy cost. Production remains blocked.

Migration `0014_local_dispatch_identity` records execution mode and whether a
tariff is synthetic, and makes authorization scopes revocable. Existing
historical calls are backfilled from their existing `test_only` classification.
Settlement reconstructs the persisted price table with this stored tariff
classification instead of assuming every test-only profile is synthetic.

Authorization checks remain strict for new Tasks, admission, send approval,
provider permit issue/consume, result adoption, and Position commits. A separate
locked observation lookup is used only for facts tied to an already admitted
operation: dispatch acceptance, provider-call and execution observations,
usage, settlement/reconciliation, and cancellation/deadline convergence. It
requires the scope row to exist and preserves the original immutable binding;
it does not make an inactive scope usable for new work. A quiescent result from
a revoked or changed authorization snapshot is recorded as a server-authored
`FAILED/POLICY` response, with the model output and any Position proposal
rejected. This policy response is limited to the matching current Task revision
after its admitted execution is proven quiescent; it does not adopt model text.
Previously committed responses, Position versions, and spending are preserved.
An unresolved execution remains UNKNOWN with its hold, and partial or
conflicting usage remains unsettled. Scope revocation alone never supplies
terminal or usage evidence.

`doctor` reports model identity separately from the runner context. `READY`
requires the exact installed model/version and a loaded runner whose reported
context is an integer at least the fixed 8192-token request gate. The pinned
model metadata context ceiling is reported separately and does not enlarge that
gate. An identity-matching installed but unloaded model can contribute to
`PRESTART_OK` when the other required checks pass and services are genuinely
not running. A loaded runner with missing, invalid, or short context, any
observed model/version/manifest mismatch, or an unavailable service is
`NOT_READY`; these are not treated as prestart conditions. Doctor uses
metadata, protocol, and health reads only and requests no model load or
generation.

The local gateway can optionally receive
`HEKATE_LOCAL_GENERATION_LEDGER`, a durable O_EXCL file under the configured
state directory. It claims the file after DB permit consumption and before
opening the upstream socket. A failure after claim stays consumed. This optional
gate is used only for the Phase 6E one-run verification, not as a historical
probe approval dependency of normal local CLI use. Normal DB permits and call
identity still enforce runtime accounting and same-key replay.

## Verification

`scripts/phase6e_local_cli_probe.py` is an integration driver, not application
runtime code. It creates isolated PostgreSQL databases, starts the ordinary
CLI, gateway, worker, and pinned App Server, and observes persistent state. The
fake preflight covers local setup replay, read-only doctor, fake CLI Task
completion, worker restart, same-key replay, and unchanged DB/gateway effects.
Only after fake preflight passes does the driver consume a fresh, worktree-local
authorization for at most one independent Qwen Task. It has no code path for a
second Qwen generation after that authorization is consumed.

The exact run command, immutable profile/image/patch identifiers, PostgreSQL
head, Task and provider-call identities, usage and settlement, replay compare,
provider request counts, pre-existing Phase 6D preservation, and final code
fingerprint are recorded in the new artifact under
`integration/runtime/artifacts/`. The artifact is the source of truth for
which gates actually passed. Do not infer a Qwen success from an earlier
Phase 6D artifact or from the fake preflight.

The revocation/doctor correction is independently exercised by
`scripts/phase6e_revocation_doctor_probe.py`. It uses a new disposable
PostgreSQL instance, the pinned Letta App Server, and a loopback fake provider
with a separate nonzero synthetic price profile. The fake request is held at
the upstream barrier while its scope is revoked, then its trusted terminal and
usage observations are processed, settled, and replayed after a worker restart.
This correction does not load Ollama or generate with Qwen; doctor context
readiness is checked with read-only response fixtures. Its fresh artifact
records the pre/post spent and held amounts, provider-call count, task-policy
response, whole-doctor classifications, and current code fingerprint.

## Operating instructions

See [the local Qwen quickstart](../local-quickstart.md) for install, migration,
private configuration, foreground startup/shutdown, ordinary `ask`/`task`, and
same-key replay commands. It describes only the tested local-candidate path;
production approval, HTTP products, operator UNKNOWN recovery, Critic/Position
Qwen validation, memory projection validation, and G7/G8 are deferred.
