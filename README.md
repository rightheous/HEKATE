# HEKATE

> **Hierarchical Epistemic Kernel for Agentic Thought and Execution**

HEKATE is a persistent personal AI orchestration system built on top of existing agent runtimes such as [Letta](https://github.com/letta-ai/letta).

## Personal local use

The personal local CLI is implemented. Start with the [personal local guide](docs/personal-use.md)
and the `config/personal-local.example` profile. The [local quickstart](docs/local-quickstart.md)
also retains the simpler setup and Phase 6F verification instructions.

The implemented path includes persistent HEKATE, bounded Critic review, PostgreSQL Task and
budget state, Evidence import, versioned Position commits, and Letta memory projection.
The architecture below describes both implemented foundations and longer-term design goals.

---

HEKATE does not attempt to build a new language model, reasoning algorithm, or memory framework.

Instead, it focuses on a different problem:

> **How should finished AI models and persistent agents be created, coordinated, revisited, stopped, constrained, and combined into a single coherent intelligence?**

The user interacts with **HEKATE only**.

Other agents are internal cognitive resources created and retired by HEKATE as needed.

---

## Why HEKATE?

Modern LLMs can already reason, plan, criticize, generate hypotheses, use tools, and maintain state when placed inside an agent runtime.

The remaining problem is increasingly one of **orchestration**.

A capable personal AI needs to decide:

- whether a problem deserves deeper reasoning,
- whether another agent should be created,
- what context that agent should receive,
- which model should perform the work,
- how much compute should be spent,
- whether another reasoning round is worthwhile,
- when reasoning should stop,
- which conclusions should persist,
- what the system currently believes,
- and what an agent is actually allowed to do.

HEKATE exists to manage those decisions.

---

# Core Idea

```text
                         USER
                           │
                           ▼
                  ┌─────────────────┐
                  │     HEKATE      │
                  │ Persistent Agent│
                  │                 │
                  │ Identity        │
                  │ Memory          │
                  │ Judgment        │
                  │ Position        │
                  └────────┬────────┘
                           │
                           ▼
               ┌──────────────────────┐
               │ HEKATE Control Plane │
               │                      │
               │ Agent Lifecycle      │
               │ Deliberation         │
               │ Scheduling           │
               │ Stopping             │
               │ Budget               │
               │ Model Routing        │
               │ Capabilities         │
               │ Sandbox Policy       │
               └──────────┬───────────┘
                          │
                          ▼
                     Agent Runtime
                        Letta
                          │
             ┌────────────┼────────────┐
             ▼            ▼            ▼
          Critic       Reasoner     Specialist
           Agent         Agent         Agent
             │            │            │
             └────────────┼────────────┘
                          ▼
                  Structured Results
                          │
                          ▼
                       HEKATE
                          │
                    Position Update
```

HEKATE itself is intended to be a **persistent Letta agent**.

The deterministic Control Plane exists separately so that an LLM can propose actions without being trusted to directly enforce permissions, mutate authoritative state, or manage its own infrastructure.

---

# Design Principles

## One user-facing intelligence

The user communicates with HEKATE, not with a collection of individually managed agents.

```text
User ↔ HEKATE
```

Subordinate agents are implementation details.

They exist only when HEKATE determines that additional cognitive work is useful.

---

## Persistent HEKATE, disposable reasoning agents

HEKATE maintains long-term identity and continuity.

Most subordinate agents are ephemeral.

```text
Need
  ↓
Spawn
  ↓
Reason
  ↓
Persist Conclusion
  ↓
Retire
  ↓
Delete
```

Agents should not accumulate indefinitely.

Persistent subordinate agents may be introduced later only when long-term specialization provides measurable value.

---

## Model judgment and system authority are different things

An LLM may decide:

> "A critic would be useful."

It does **not** directly gain the authority to create arbitrary processes, access arbitrary files, or modify authoritative state.

Instead:

```text
HEKATE
  ↓ proposal
Control Plane
  ↓ policy / budget / permission checks
Execution
```

The same principle applies to Position updates, tool execution, network access, filesystem access, and agent lifecycle operations.

---

# Position

A central HEKATE concept is **Position**.

A Position is:

> **HEKATE's current, system-level, revisable judgment about a topic.**

It is not simply the user's opinion.

It is not the opinion of a single subordinate agent.

It is not permanent truth.

```text
Evidence
+ Constraints
+ Agent Assessments
+ Previous Position
        │
        ▼
   HEKATE Judgment
        │
        ▼
      Position
```

A Position may contain:

- statement,
- scope,
- assumptions,
- evidence references,
- uncertainty,
- dissent,
- provenance,
- version history.

HEKATE may therefore disagree with the user while still acting in service of the user's goals.

The desired property is not opposition.

It is **epistemic independence**.

---

# Local Agent Stances

Subordinate agents do not own HEKATE Position.

Their conclusions are represented using concepts such as:

- `stance`
- `assessment`
- `hypothesis`
- `proposal`

For example:

```text
Agent A → assessment
Agent B → hypothesis
Agent C → objection
              │
              ▼
          HEKATE synthesis
              │
              ▼
            Position
```

Only HEKATE may propose adopting or changing the swarm-level Position.

The Control Plane validates and commits the authoritative version.

---

# Deliberation

HEKATE is designed to support **adaptive reasoning depth**.

A simple question should remain simple:

```text
User
→ HEKATE
→ Answer
```

A difficult question may expand into a deliberation process:

```text
Problem
  ↓
HEKATE
  ↓
Need more reasoning?
  ├─ No  → Answer
  │
  └─ Yes
       ↓
   Spawn agent
       ↓
   Assessment
       ↓
   HEKATE evaluates
       ↓
   More useful work?
       ├─ Yes → Continue
       └─ No  → Stop / Commit / Abstain
```

HEKATE does **not** assume that more reasoning is always better.

The important question is:

> **Is another inference likely to improve the decision enough to justify its cost?**

---

# Stopping

A system capable of thinking repeatedly also needs to know when to stop.

Potential stop signals include:

- conclusion stability,
- no significant new evidence,
- no unresolved high-severity objection,
- lack of a concrete next reasoning action,
- diminishing returns,
- token or monetary budget exhaustion,
- deadlines,
- cancellation,
- resource limits.

Stopping and accepting a Position are intentionally separate operations.

HEKATE may stop deliberation and still conclude:

```text
ABSTAIN
```

or:

```text
PROVISIONAL_ANSWER
```

rather than pretending uncertainty has disappeared.

---

# Reasoning Agents

HEKATE may eventually create agents with different reasoning roles.

Potential roles include:

### Deductive

Tests whether conclusions follow from known premises and constraints.

### Inductive

Looks for patterns and generalizations from observations.

### Abductive

Generates plausible explanations or alternative hypotheses.

### Critic / Falsifier

Attempts to identify hidden assumptions, contradictions, counterexamples, and failure conditions.

These roles are **prompt and task framings**, not separate reasoning algorithms implemented by HEKATE.

The underlying intelligence remains the model.

---

# v0.1: Start With One Critic

HEKATE v0.1 intentionally does **not** begin with a large swarm.

The first subordinate reasoning role is a single ephemeral **Critic**.

Its job is to test whether adding a separate reasoning process provides enough value to justify the additional complexity and cost.

Initial comparison:

```text
HEKATE baseline
        vs
HEKATE + additional self-reasoning
        vs
HEKATE + Critic
        vs
stronger single model
```

If the Critic does not provide measurable value, it should not be kept merely because multi-agent architecture is interesting.

---

# Task Capsules

A subordinate agent should not receive the entire HEKATE memory and conversation history.

Instead, HEKATE provides a bounded **Task Capsule** containing only what is needed.

Example:

```yaml
task:
  objective: "Review the current architecture for major failure conditions."

role:
  type: "critic"

premises:
  - "HEKATE is a persistent Letta agent."
  - "The Control Plane owns authoritative operational state."

evidence_refs:
  - E12
  - E18

target_position:
  version: 4
  summary: "..."

constraints:
  - "Do not treat unverified claims as facts."

runtime_limits:
  max_output_tokens: 3000
  max_tool_calls: 3
```

This reduces:

- context size,
- token cost,
- irrelevant memory,
- accidental information leakage,
- anchoring,
- prompt instability.

---

# Conclusion Capsules

Agents return structured conclusions rather than forcing HEKATE to replay their complete internal history.

Example:

```yaml
status: done

assessment:
  statement: >
    Agent creation recovery is underspecified and may produce duplicates.

  confidence:
    level: high

evidence_used:
  - E12

objections:
  - severity: high
    claim: >
      A lost provider response could result in duplicate agent creation.

unresolved: []

position_recommendation:
  action: modify
```

The durable result matters.

The temporary agent does not.

---

# Memory Is Not Context

HEKATE distinguishes between everything it remembers and what a model sees during one inference.

```text
Memory
= total persistent state

Context
= information currently brought into an inference
```

A conceptual hierarchy:

```text
L0 — Identity
L1 — Active task state
L2 — Retrieved long-term state
L3 — Archive
```

Not every historical Position, source document, agent transcript, or conversation belongs in every prompt.

---

# Deliberative Sleep

Future versions of HEKATE may support **deliberative sleep**.

This is different from ordinary memory consolidation.

```text
Unresolved problem
      ↓
Background reasoning
      ↓
Alternative hypothesis
      ↓
Re-evaluation
      ↓
Possible Position update
```

Sleep may run when:

- the system is idle,
- new evidence appears,
- a scheduled reconsideration becomes due,
- an unresolved task remains worth revisiting.

Background reasoning must remain bounded by budget, cancellation, revision, and Position-version checks.

HEKATE must never think indefinitely simply because compute is available.

---

# Agent Lifecycle

HEKATE manages subordinate agents explicitly.

Example lifecycle:

```text
REQUESTED
    ↓
CREATING
    ↓
READY
    ↓
BUSY
    ↓
RETIRING
    ↓
DELETE_PENDING
    ↓
DELETED
```

Task completion and agent lifecycle are separate concepts.

A completed task does not automatically imply that the underlying agent object has already been safely deleted.

Durable results are persisted before retirement.

---

# Security

HEKATE follows a **sandbox-by-default, least-privilege** model inspired by systems such as Moltis.

Core principles:

- deny by default,
- explicit capability grants,
- scoped resource access,
- mediated tool execution,
- filesystem restrictions,
- network restrictions,
- secret isolation,
- bounded CPU/memory/time,
- auditable mutation,
- no uncontrolled recursive spawning.

A reasoning agent should normally require very little authority.

Example default profile:

```yaml
profile: reasoning-readonly

evidence:
  mode: scoped

filesystem:
  mode: none

network:
  mode: deny

shell:
  allowed: false

process_spawn:
  allowed: false

agent_management:
  allowed: false

secrets:
  raw_access: false

mutations:
  allowed: false
```

Subordinate agents do not directly create or delete other agents.

They may request additional reasoning from HEKATE.

---

# Cost Strategy

HEKATE is an orchestration system.

Without explicit cost control, deeper reasoning can easily become uncontrolled repeated inference.

The main cost strategies are:

## Prompt Caching

Keep reusable prompt prefixes stable where supported.

```text
stable:
  identity
  policies
  tool definitions
  role templates

volatile:
  task
  evidence
  active state
```

## Multi-Model Routing

Use stronger models only when their value justifies their cost.

```text
cheap
  ↓ insufficient
medium
  ↓ insufficient
strong
```

## Context Minimization

Use Task Capsules and scoped retrieval instead of copying full memory.

## Selective Spawning

Do not create an agent merely because one can be created.

## Result Reuse

Reuse still-valid structured conclusions when their evidence, constraints, and revisions remain compatible.

## Budget-Aware Stopping

Reasoning consumes explicit budgets:

- tokens,
- API cost,
- wall-clock time,
- agent count,
- reasoning rounds,
- local compute.

---

# Letta

HEKATE uses Letta as its persistent agent substrate.

Conceptually, Letta is responsible for:

- persistent agents,
- agent memory,
- conversation state,
- model execution primitives,
- agent runtime behavior.

HEKATE is responsible for the layer above it:

```text
Who should think?
When?
With what context?
With what permissions?
Using which model?
For how long?
At what cost?
When should we stop?
What conclusion should persist?
```

The exact Letta integration boundary must be validated against the deployed Letta version before implementation contracts are frozen.

HEKATE should use supported Letta interfaces rather than depending directly on Letta's internal database representation.

---

# Control Plane

The HEKATE Control Plane is deterministic application infrastructure.

It is expected to own:

- Task state
- Execution attempts
- Agent registry
- Position versions
- Evidence metadata
- Budget ledger
- Schedules
- Audit events
- Lifecycle reconciliation
- Capability enforcement

The Control Plane does not decide whether a hypothesis is intellectually correct.

It determines whether a proposed operation is **valid, authorized, affordable, current, and safe to execute**.

---

# State Ownership

Each important type of state has one authoritative owner.

| State | Authoritative owner |
|---|---|
| HEKATE / agent conversational memory | Letta |
| Task state | HEKATE application store |
| Execution attempts | HEKATE application store |
| Agent lifecycle metadata | HEKATE application store |
| Actual Letta agent existence | Letta |
| HEKATE Position history | HEKATE application store |
| Budget ledger | HEKATE application store |
| Capability policy | trusted HEKATE configuration |
| Evidence metadata | HEKATE application store |
| Original retained sources | archive storage |

Duplicate projections may exist for convenience.

They are not allowed to become competing sources of truth.

---

# Failure Is Part of the Design

HEKATE does not assume external operations are exactly-once.

The system must tolerate situations such as:

- agent creation succeeds but the response is lost,
- a worker crashes after dispatch,
- a result arrives twice,
- cancellation occurs but a result arrives later,
- deletion fails,
- Position changed while an old result was running,
- usage remains unknown after timeout,
- a budget reservation exists without final settlement.

Operations therefore require durable identifiers, revision checks, reconciliation, and idempotent state transitions.

---

# Evaluation

Multi-agent architecture is not assumed to be better.

It must earn its complexity.

HEKATE should measure:

### Quality

- correctness,
- critical error detection,
- unsupported claims,
- false objections,
- appropriate abstention,
- evidence traceability.

### Restraint

- unnecessary agent spawning,
- unnecessary reasoning rounds,
- repeated reasoning,
- unjustified Position changes.

### Cost

- input/output tokens,
- cache usage,
- API cost,
- local compute,
- latency.

### Reliability

- completion rate,
- orphan agents,
- stale commit rejection,
- cancellation handling,
- duplicate result handling,
- deletion success,
- recovery after restart.

---

# v0.1 Scope

The first implementation should remain deliberately small.

### Required

- persistent HEKATE
- Letta integration adapter
- one ephemeral Critic
- durable application state
- Task Capsule
- Conclusion Capsule
- Position versioning
- evidence references
- task / attempt / agent lifecycle separation
- bounded deliberation
- budget accounting
- capability enforcement
- cancellation
- recovery / reconciliation
- observability
- comparative evaluation

### Not required yet

- full reasoning swarm
- deduction / induction / abduction agents
- persistent specialists
- automatic background deliberation
- complex dynamic model routing
- semantic result reuse
- recursive spawning
- autonomous self-modification
- unrestricted shell execution
- large-scale agent society

---

# Roadmap

## Phase 0 — Letta Integration Validation

Verify the actual runtime contract.

```text
Create persistent agent
→ Send message
→ Reconnect
→ Confirm persisted state
→ Invoke controlled tool
→ Observe tool execution boundary
→ Test errors / timeout / concurrency
→ Create ephemeral agent
→ Delete agent
```

Document supported and unsupported capabilities before designing around them.

---

## Phase 1 — Persistent HEKATE

Establish the primary HEKATE identity and durable conversation path.

---

## Phase 2 — Safe Critic Path

Allow HEKATE to request one limited Critic, collect its structured result, and safely retire it.

---

## Phase 3 — Position and Recovery

Implement authoritative Position history, evidence references, cancellation, stale-result protection, and reconciliation.

---

## Phase 4 — Bounded Deliberation

Allow HEKATE to autonomously decide whether another reasoning step is useful while enforcing hard external limits.

---

## Phase 5 — Evaluation

Compare HEKATE against:

- baseline HEKATE,
- additional single-agent reasoning,
- HEKATE + Critic,
- a stronger single model.

---

## Phase 6 — Measured Expansion

Only after measurement, consider:

- Deductive agents
- Inductive agents
- Abductive agents
- multi-model routing
- deliberative sleep
- persistent specialists
- semantic result reuse
- richer sandboxed execution

---

# Architectural Invariants

HEKATE v0.1 should preserve the following rules:

1. The user-facing AI identity is HEKATE.
2. HEKATE alone proposes swarm-level Position changes.
3. Authoritative Position changes pass through the Control Plane.
4. Position history is versioned and immutable.
5. Stale results cannot silently overwrite current state.
6. Subordinate agents cannot create or delete agents directly.
7. Agent lifecycle operations pass through the Control Plane.
8. Capabilities are enforced at execution boundaries, not merely described in prompts.
9. Subordinate agents do not receive the complete HEKATE context by default.
10. Useful subordinate results are persisted before agent deletion.
11. Agent memory and authoritative application state remain distinct.
12. Paid or bounded work requires budget authorization.
13. Retries and repair attempts consume budget.
14. Late results from cancelled work are never silently adopted.
15. Stopping deliberation does not require adopting a Position.
16. Model judgment, user authorization, and system enforcement remain separate.

---

# Project Philosophy

HEKATE is not an attempt to simulate a human brain.

It is not an attempt to invent another reasoning algorithm.

It is not a collection of agents for the sake of having many agents.

HEKATE treats capable models as **finished cognitive components**.

The engineering problem is to use those components well.

> **Letta gives an agent persistence.  
> HEKATE decides how intelligence should be used.**

The goal is a system that can think deeply when necessary, remain simple when it is not, disagree when evidence demands it, stop when further thought is no longer useful, and remain under the user's authority throughout the process.

---

## Status

The supported personal local Qwen path is implemented and has been exercised through the CLI,
including persisted answers, usage settlement, and replay without extra provider requests.
See the [personal local guide](docs/personal-use.md) for the supported commands and operating limits.

Local mode uses an explicitly selected candidate profile and a private loopback runtime.
Production dispatch remains disabled. HTTP product APIs, operator recovery for unknown
executions, and hosted-provider dispatch are outside the current scope. Web research is not
connected in the current personal profile.

The recorded Phase 6F Goal-wide generation limit was exceeded. The personal profile is a
separate implementation path and does not change that historical result; see the
[Phase 6F correction](docs/implementation/phase6f-submission-review.md).
