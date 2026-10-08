# Phase 1 handoff and Phase 2 entry conditions

## Evidence and gate state

Phase 1 keeps a pinned local Letta App Server, TypeScript bridge, permit-checking fake gateway, and synthetic provider. It makes no real provider calls and adds no database, production budget ledger, scheduler, or Critic lifecycle.

The design inputs `HEKATE_design_v0_1.md`, `01-Letta-조사와-언어-판단.md`, `02-구현-상세설계.md`, and `03-통합검증과-구현순서.md` are not present in this checkout. Restore them before using this handoff to settle any semantic conflict with the source spec.

| Gate | Phase 1 result | Evidence and limit |
|---|---|---|
| G1 | Pass | SDK 0.8.25 connects to Letta Code 0.33.8, protocol 1. |
| G2 | Pass | Tagged create, lookup, list, delete; registry ID and provider ID remain distinct. |
| G5 | Pass for this profile | Session toolset is disabled and the network/tool routes below were audited. Applies only to this isolated local App Server profile. |
| G6 | Pass | Fake gateway checks binding, model, output ceiling, call kind, expiry, and single use before forwarding. This is not a production atomic ledger. |
| G7 | Blocked | Same-execution transport resume is unsupported. App Server process loss after dispatch is surfaced as `UNKNOWN`; the probe does not retry. |
| G8 | Blocked | No approved real model and tokenizer profile exists for exact full-request input accounting. |
| G9 | Pass with pending rows | All successful authorized turn and compaction calls match synthetic usage by accounting ID and provider response ID. The HTTP 500 and post-dispatch process-loss calls remain `EXPECTED_PENDING` (2 rows), with 0 blocking mismatches or missing usage. This is not billing evidence. |

Final evidence: `integration/letta/artifacts/p1-20260930T132408Z-907c3d1d.json`; the capability matrix points to this artifact. The run recorded 11 runtime provider requests, 1 non-runtime gateway request, 12 provider endpoint requests total, and 0 real provider calls. Probe containers and internal networks were removed after the run.

The local runtime emits usage before `stop_reason`. The SDK's 100 ms trailing-usage grace for hosted streams that put usage after `stop_reason` is source-checked, but that ordering cannot be exercised by this local provider path. Bridge event collection stores same-call merges, so it does not count raw duplicate/late SDK deliveries.

## G5 tool and network reachability audit

| Surface | Registration and handler | Reachability in this profile | Evidence |
|---|---|---|---|
| Bash/shell | Client tool → `client_tools` → `toPiTools()` → provider call | Not exposed | `session.prepare` sends `allowedTools: []`, `toolset: {base: "none"}`, and `tools: []`; provider tool arrays were empty before and after reprepare. An unoffered tool call produced zero executor calls. |
| File read/write | Client tools through the same provider/tool executor path | Not exposed | Same disabled session toolset and empty provider tool list. No filesystem tool is registered for the model. |
| Web and general network | Client tools; App Server egress | No web tool; general egress blocked | App Server runs on an internal Docker network. The only host route is the fake gateway; it requires an issued permit for POSTs and returns synthetic responses. |
| MCP | Interactive CLI MCP client and its registered client tools | Not exposed | Probe state contains only provider auth configuration; no MCP server is configured or attached to the disabled session toolset. MCP is not registered by the local App Server backend path. |
| Native subagent | Agent/Task client tool → CLI subagent manager | Not exposed | No Agent/Task tool is attached to the session. The negative unoffered-tool request did not reach an executor. |

Static route evidence is in `bridge/letta/src/main.ts`, `src/backend/dev/pi-stream-adapter.ts`, and the local App Server startup in `scripts/integration_probe.py`. Dynamic evidence is in the capability artifact referenced by `integration/letta/capability-matrix.json`. Do not extend the G5 claim to a different backend, non-isolated App Server, or session with tools enabled.

## Phase 2 record identities

These are minimum identity and state fields for implementation planning, not a migration or a claim that a production ledger exists. Keep an operation result eligible for acceptance separate from usage settlement.

| Record | Identity key | Required fields and rule |
|---|---|---|
| Operation journal | `operation_id` (unique); retain `(task_id, attempt_id)` as the owning scope | `task_id`, `attempt_id`, `agent_registry_id`, `provider_agent_id`, `input_revision`, `fence`, `conversation_id`, operation kind, dispatch state, result state, timestamps. A capsule agent ID is a claim; compare it to the trusted registry binding. |
| Reservation | `reservation_id`; unique per operation and reserved call kind | `operation_id`, `call_kind`, model/profile version, input and output ceilings, reservation state, expiry. Reserve atomically with operation admission; retain or quarantine it while dispatch outcome is unknown. |
| Provider call | `accounting_call_id` (unique per physical request) | `operation_id`, `reservation_id`, `call_kind`, model/profile version, `provider_call_id` nullable, dispatch/result state, request and response times. One operation may have multiple calls, including compaction. Do not derive this ID from `provider_call_id`. |
| Usage | `accounting_call_id` (one reconciliation target per physical request) | `provider_call_id` nullable, source, completeness, input/output/total tokens, cost and currency when reported, observation time. Same-call updates fill missing fields; contradictory fields become a mismatch and never overwrite settled values. |

`MATCHED`, `EXPECTED_PENDING`, `MISSING_USAGE`, `MISMATCH`, `NOT_FORWARDED`, and `FORWARDING_UNKNOWN` describe reconciliation. `UNKNOWN` describes execution outcome. A denied pre-forward request is `NOT_FORWARDED`; a dispatched operation with a lost stream is `UNKNOWN` and its usage remains pending. Do not mark it failed, release its reservation, or retry automatically based only on transport loss.

Result eligibility checks schema, trusted task/attempt/revision/agent binding, and then reference validity. Usage settlement checks the physical provider call and observed usage independently. A result can be eligible while settlement is pending; a usage record cannot make an ineligible result acceptable.

## Conditions before database work

Database work can begin once the operation identity, binding, physical-call ID, and pending-settlement semantics above are confirmed against the restored HEKATE spec; G8 real-provider readiness is a separate gate. Implement admission and reservation in one transaction with unique constraints for operation identity and provider-call identity. Persist state transitions and retain an auditable mismatch instead of overwriting evidence. Do not let a database lookup imply that an Evidence reference exists or is currently accessible unless that reference check ran.

## Conditions before real provider execution

G8 stays blocked until a selected v0.1 model has a pinned provider/model configuration and a validated tokenizer that counts the entire effective request: system prompt, tool schemas, memory, and history. Reserve output tokens from the context window and reject an over-bound request before forwarding. Then prove the production permit check is atomic, single-use, and correlated to every physical call, including compaction. Keep unknown transport outcomes pending and do not resend them automatically. Only after those conditions and an explicit real-provider test plan should Phase 2 exercise a real provider.
