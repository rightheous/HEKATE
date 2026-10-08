# HEKATE v0.1 Architecture Specification

**Status:** Revised Design Freeze Candidate  
**Version:** v0.1  
**Project:** HEKATE  
**Foundation:** Letta persistent agents  
**Purpose:** Personal AI orchestration and deliberation system

---

# 1. 문서 목적

HEKATE v0.1은 기존 HEKATE 설계를 계승하지 않는 clean-slate architecture다.

HEKATE는 새로운 LLM runtime, memory framework, 추론 알고리즘을 처음부터 구현하지 않는다.

Letta를 persistent agent substrate로 사용하고, 그 위에서 다음을 담당하는 orchestration layer를 구현한다.

- 사용자와의 단일 인터페이스
- persistent identity 및 conversation continuity
- subordinate agent 생성·관리·삭제
- 선택적 multi-agent deliberation
- 시스템 차원의 Position 관리
- deliberation scheduling 및 종료
- budget 및 resource enforcement
- capability 및 tool permission enforcement
- 장애 복구와 상태 정합성
- decision provenance 및 observability

후속 버전에서는 다음을 확장할 수 있다.

- 다양한 reasoning role
- multi-model routing
- persistent subordinate agents
- background deliberation
- semantic result reuse

HEKATE가 해결할 문제는 다음과 같다.

> 지능 모델과 persistent agent를 언제, 얼마나, 어떤 맥락과 권한으로 실행하고, 어떤 결과를 채택하며, 어떻게 안전하게 중단하고 정리할 것인가?

---

# 2. 설계 범위와 전제

## 2.1 지능과 실행 제어를 구분한다

LLM은 다음 작업을 수행하는 판단 주체로 사용한다.

- 사용자 의도 이해
- 문제 분석
- hypothesis generation
- critique
- evidence interpretation
- 결과 종합
- 추가 작업 제안
- Position 채택 또는 변경 제안

이러한 능력이 항상 정확하다고 가정하지 않는다.

LLM의 출력은 검증 대상이며, 시스템 상태 변경 권한이나 보안 정책을 우회할 수 없다.

## 2.2 Letta의 실제 지원 범위는 구현 전에 검증한다

본 문서는 HEKATE가 요구하는 아키텍처 계약을 정의한다.

특정 Letta 버전이 다음 기능을 어떤 방식으로 지원하는지는 integration spike에서 확인한다.

- agent creation 및 deletion
- persistent memory
- message delivery
- client-side 또는 server-side tool execution
- asynchronous execution
- usage reporting
- cancellation
- agent metadata 및 enumeration
- model configuration
- concurrent turn 제약

지원되지 않는 기능은 adapter 또는 Control Plane에서 보완하거나, 해당 기능의 범위를 축소한다.

기능 이름이나 개념적 유사성만으로 지원을 가정하지 않는다.

---

# 3. 핵심 철학

## 3.1 사용자는 HEKATE하고만 상호작용한다

외부적으로 보이는 AI identity는 HEKATE 하나다.

```text
User
  ↕
HEKATE
  │
  └─ On-demand subordinate agents
```

Subordinate agent는 내부 계산 자원이다.

사용자는 subordinate agent를 직접 생성하거나 대화 상대별로 관리할 필요가 없다.

다만 비용, 작업 진행 상태, 사용된 근거, 중요한 dissent는 필요한 수준에서 설명할 수 있어야 한다.

## 3.2 기본값은 단일 HEKATE다

기본 실행 경로는 다음과 같다.

```text
User → HEKATE → Answer
```

모든 요청에 multi-agent deliberation을 적용하지 않는다.

추가 agent는 특정 오류를 탐지하거나 중요한 불확실성을 줄일 합리적인 이유가 있을 때만 생성한다.

## 3.3 Agent는 관리해야 하는 자원이다

기본 subordinate agent는 ephemeral이다.

```text
Need
→ Spawn
→ Work
→ Persist result
→ Retire
→ Delete
```

실행 객체를 삭제해도 필요한 결론과 provenance는 보존한다.

## 3.4 독립적 판단과 사용자 통제권을 구분한다

사용자의 주장은 자동으로 사실이 되지 않는다.

그러나 사용자가 정한 목표, 선호, 예산, 권한 및 승인 조건은 허용된 작업 범위를 결정한다.

```text
User factual claim ≠ verified fact
User preference ≠ factual claim
HEKATE Position ≠ execution authorization
```

HEKATE는 사실 판단에서는 독립성을 유지하되, 사용자에게 부여받지 않은 실행 권한을 스스로 만들지 않는다.

---

# 4. 시스템 구성과 책임

## 4.1 HEKATE Agent

사용자가 관계를 맺는 persistent Letta agent다.

담당:

- 사용자 의도 이해
- 직접 응답 여부 판단
- 추가 deliberation 제안
- subordinate task 작성
- 결과 종합
- Position commit 제안
- 판단 보류 및 추가 정보 요청
- 사용자에게 결과와 한계 설명

HEKATE Agent는 정책상 허용 여부를 최종 집행하지 않는다.

## 4.2 HEKATE Control Plane

deterministic application runtime이다.

담당:

- agent lifecycle
- task 및 attempt 상태 관리
- scheduling
- budget reservation 및 settlement
- permissions
- tool execution mediation
- timeout 및 cancellation
- structured state persistence
- Position commit validation
- result collection
- reconciliation
- observability

Control Plane은 LLM의 사실 판단을 대체하지 않는다.

## 4.3 Letta

HEKATE와 subordinate agents의 다음 기반을 제공하는 substrate로 사용한다.

- conversation
- agent memory
- agent persistence
- agent execution primitives

실제 보장 범위는 선택한 배포 구성 및 버전으로 검증한다.

## 4.4 Durable Application Store

HEKATE의 authoritative operational state를 저장한다.

최소 대상:

- tasks
- attempts
- agent registry
- budget ledger
- Position versions
- evidence metadata
- capsules
- schedules
- audit events

v0.1에서는 단일 transactional database로 시작할 수 있다.

이는 새로운 general-purpose memory engine이 아니라 애플리케이션 상태 저장소다.

---

# 5. State Ownership

각 데이터는 하나의 authoritative source를 가진다.

| 데이터 | Authoritative source | 다른 위치의 표현 |
|---|---|---|
| Agent conversation 및 개인화 memory | Letta | 필요 시 reference |
| Task 및 attempt 상태 | Application Store | HEKATE context의 요약 |
| Agent lifecycle metadata | Application Store | provider 상태를 관찰해 reconciliation |
| 실제 provider agent 존재 여부 | Letta | registry에 마지막 관찰값 기록 |
| Position 및 history | Application Store | Letta memory의 요약·reference |
| Budget ledger | Application Store | UI 및 context의 요약 |
| Evidence metadata | Application Store | capsule의 reference |
| 보존하는 원문·artifact | Archive storage | evidence locator |
| Capability policy | Trusted policy configuration | agent에게 설명용 요약 |

중복 표현은 가능하지만 authoritative source를 중복으로 두지 않는다.

Letta memory에 저장된 Position 요약은 authoritative Position이 아니다.

---

# 6. HEKATE Position

## 6.1 정의

Position은 다음과 같다.

> HEKATE가 evidence, constraints 및 subordinate assessments를 검토한 뒤 채택한 시스템 차원의 수정 가능한 판단.

Position은 모든 agent의 합의를 의미하지 않는다.

Position은 다음을 가진다.

- 명확한 topic 및 scope
- statement
- 적용 조건
- evidence references
- assumptions
- dissent
- uncertainty
- provenance
- version

모든 사용자 응답을 Position으로 저장할 필요는 없다.

장기적으로 재사용하거나 변경 이력을 추적할 가치가 있는 판단만 Position으로 관리한다.

## 6.2 Agent-local Stance

Subordinate agent의 판단은 다음 용어를 사용한다.

- stance
- assessment
- hypothesis
- proposal

Subordinate agent는 Position을 직접 수정할 수 없다.

```text
Subordinate assessment
→ HEKATE synthesis
→ HEKATE commit proposal
→ Control Plane validation
→ Position version
```

## 6.3 Confidence

v0.1의 기본 confidence 표현은 정성적이다.

```yaml
confidence:
  level: "medium"
  basis:
    - "핵심 제약을 충족하지만 성능 측정은 수행되지 않았다."
  missing_evidence:
    - "실제 workload benchmark"
```

Confidence는 calibrated probability가 아니다.

Agent 간 confidence를 평균하여 최종 정답 확률로 사용하지 않는다.

---

# 7. Position Commit Protocol

## 7.1 의미적 권한과 저장 실행을 구분한다

- HEKATE만 Position 채택·변경을 제안할 수 있다.
- Control Plane만 authoritative store에 commit을 실행한다.
- Control Plane은 권한, schema, version, task validity 및 reference integrity를 검증한다.

Control Plane이 commit을 저장한다는 것은 독립적으로 판단 내용을 채택한다는 뜻이 아니다.

## 7.2 Commit Request

```yaml
position_commit:
  operation_id: "op_commit_0042"
  task_id: "dt_0023"
  topic_id: "backend_architecture"
  base_version: 10
  input_revision: 7

  proposed_position:
    statement: "현재 규모에서는 Python 단일 구현을 우선한다."
    applicability:
      - "현재 성능 요구사항을 유지하는 동안"
    confidence:
      level: "medium"
      basis:
        - "개발 비용과 운영 복잡도를 우선했다."
    evidence_refs:
      - "E12"
    assumptions:
      - "향후 3개월 내 처리량이 급증하지 않는다."
    dissent_refs:
      - "D4"

  reason_for_change: "운영 인력과 일정 제약이 갱신되었다."
```

## 7.3 Validation

Commit은 다음 조건을 만족해야 한다.

1. 요청 주체가 HEKATE다.
2. 해당 task가 commit 가능한 상태다.
3. 사용자 입력 및 constraints revision이 유효하다.
4. `base_version`이 현재 Position version과 일치한다.
5. 참조된 evidence와 dissent가 존재하고 접근 가능하다.
6. 동일 `operation_id`가 중복 적용되지 않는다.

## 7.4 Atomic Commit

하나의 transaction에서 다음을 처리한다.

- version 비교
- 새 immutable Position version 저장
- current pointer 변경
- commit event 기록

Version conflict가 발생하면 자동 overwrite하지 않는다.

현재 상태를 다시 읽고 재평가하거나 판단을 보류한다.

## 7.5 Memory Projection

Commit 이후 Letta memory 및 active context의 요약을 갱신한다.

Projection 실패는 authoritative commit을 취소하지 않는다.

실패한 projection은 재시도하며, 중요 판단에 사용하기 전 authoritative version을 확인한다.

---

# 8. Evidence Model

Evidence reference는 단순 문자열이 아니라 검증 가능한 source record를 가리킨다.

```yaml
evidence:
  id: "E12"
  kind: "external_observation"
  source_uri: "archive://documents/design-review-01"
  locator: "section 3"
  retrieved_at: "2026-01-01T10:00:00Z"
  observed_at: null
  content_hash: "sha256:..."
  derived_from: []
  access_scope: "project_hekate"
  retention_class: "project"
```

## 8.1 Evidence Kind

최소한 다음을 구분한다.

- `external_observation`
- `user_claim`
- `user_preference`
- `model_hypothesis`
- `agent_assessment`
- `prior_position`
- `derived_summary`

출처가 있다고 해서 내용의 진실성이 자동으로 보장되는 것은 아니다.

## 8.2 Evidence Independence

여러 agent가 같은 source를 반복 인용한 경우 독립적인 evidence가 증가한 것으로 보지 않는다.

모델이 생성한 가설이나 agent assessment는 외부 관찰과 구분한다.

Derived summary는 가능하면 원래 source까지 추적할 수 있어야 한다.

## 8.3 Evidence Access and Retention

Evidence reference를 전달했다고 접근 권한이 자동으로 부여되지는 않는다.

Control Plane은 조회 시 scope를 검사한다.

원문 삭제 또는 보존 기간 만료 후에는 다음을 구분해서 표시한다.

- reference 존재
- 원문 조회 가능
- 재검증 가능

민감 데이터는 provenance 목적만으로 무기한 보존하지 않는다.

---

# 9. Deliberation Task와 Execution Attempt

Task는 해결하려는 문제이고, attempt는 특정 실행 시도다.

하나의 task에 retry 또는 replacement attempt가 존재할 수 있다.

```yaml
deliberation_task:
  id: "dt_0023"
  question: "현재 설계에서 가장 중요한 실패 가능성은 무엇인가?"
  status: "running"

  input_revision: 7
  topic_id: "backend_architecture"
  base_position_version: 10

  unresolved:
    - "장애 복구 시 중복 실행 가능성"

  attempts:
    - "at_001"

  outcome: null
  stop_reason: null
```

```yaml
execution_attempt:
  id: "at_001"
  task_id: "dt_0023"
  agent_registry_id: "ha_0042"
  status: "running"
  dispatch_operation_id: "op_dispatch_001"
  budget_reservation_id: "br_001"
  deadline_at: "..."
```

## 9.1 Task States

```text
QUEUED
RUNNING
WAITING
STOPPING
COMPLETED
FAILED
CANCELLED
```

## 9.2 Attempt States

```text
PENDING
DISPATCHED
RUNNING
SUCCEEDED
FAILED
TIMED_OUT
CANCELLED
```

Task outcome와 agent lifecycle을 혼합하지 않는다.

---

# 10. Task Capsule

Subordinate agent에게 HEKATE 전체 context를 전달하지 않는다.

```yaml
task_capsule:
  schema_version: "1"
  task_id: "dt_0023"
  attempt_id: "at_001"
  input_revision: 7

  objective: "현재 설계의 주요 실패 조건을 검토하라."
  reasoning_role: "critic"
  mode: "targeted_review"

  premises:
    - id: "P1"
      text: "Control Plane은 단일 서비스로 시작한다."
      kind: "design_constraint"

  evidence_refs:
    - "E12"

  target_position:
    topic_id: "backend_architecture"
    version: 10
    summary: "..."

  constraints:
    - "검증되지 않은 사실을 외부 관찰처럼 제시하지 않는다."
    - "반론의 성립 조건을 명시한다."

  expected_output:
    schema: "conclusion_capsule_v1"

  runtime_limits:
    max_output_tokens: 3000
    max_tool_calls: 3
    deadline_at: "..."

  capability_profile: "reasoning-readonly"
```

Capsule에 적힌 runtime limit은 설명용이다.

실제 enforcement 값은 Control Plane이 보유한 task policy에서 가져온다.

## 10.1 Context Modes

### Independent exploration

현재 Position을 제공하지 않고 문제, evidence, constraints를 전달한다.

목적은 초기 결론에 대한 anchoring을 줄이는 것이다.

### Targeted review

검토할 Position과 반론 대상 claim을 명시한다.

v0.1의 Critic은 targeted review를 기본으로 한다.

Role 분리만으로 추론의 독립성이 보장된다고 가정하지 않는다.

---

# 11. Conclusion Capsule

전체 conversation 대신 구조화된 결과를 회수한다.

```yaml
conclusion_capsule:
  schema_version: "1"
  task_id: "dt_0023"
  attempt_id: "at_001"
  agent_id: "ha_0042"

  status: "done"

  assessment:
    statement: "중복 생성 및 늦은 결과에 대한 처리 규칙이 필요하다."
    confidence:
      level: "high"
      basis:
        - "정상 경로 외의 상태 전이가 정의되어 있지 않다."

  evidence_used:
    - "E12"

  objections:
    - id: "O1"
      severity: "high"
      claim: "생성 응답 유실 시 agent가 중복 생성될 수 있다."
      condition: "provider가 생성 요청의 idempotency를 보장하지 않을 때"
      suggested_validation: "응답 유실 fault injection"

  assumptions: []
  unresolved: []

  recommended_next_step:
    type: "none"

  position_recommendation:
    action: "modify"
    summary: "장애 복구 계약을 추가한다."
```

## 11.1 Validation Layers

Validation은 구분한다.

1. Schema validation
2. Task/attempt identity validation
3. Evidence reference validation
4. Semantic evaluation by HEKATE

Schema가 올바르다고 결론이 사실인 것은 아니다.

## 11.2 Durable Result First

결과를 durable storage에 저장한 뒤 retirement를 진행한다.

```text
Receive
→ Validate
→ Persist
→ Mark collectable
→ HEKATE evaluation
→ Retire
```

HEKATE가 일시적으로 unavailable이어도 결과는 보존한다.

---

# 12. Reasoning Roles

## 12.1 v0.1 Required Role

v0.1에서는 Critic 하나만 구현한다.

목적:

- hidden assumption 탐지
- constraint violation 검토
- counterexample 제안
- 중요한 실패 조건 식별
- 기존 결론을 변경할 만한 반론 제시

반대 자체를 목표로 하지 않는다.

## 12.2 Future Roles

측정 결과에 따라 다음을 추가할 수 있다.

- Deductive
- Inductive
- Abductive
- Domain specialist

역할 이름은 prompt 및 task framing을 의미한다.

서로 다른 role이 서로 독립된 reasoning engine이나 검증기를 의미하지는 않는다.

---

# 13. Spawn Policy

Spawn에는 구체적인 목적이 필요하다.

가능한 사유:

- unresolved contradiction
- high-severity design risk
- competing explanations
- 검증 가능한 중요한 반론
- 오류 비용이 큰 판단에 대한 추가 검토

낮은 confidence나 높은 stakes는 추가 작업을 검토할 신호다.

그 자체로 multiple agent spawning을 자동 정당화하지 않는다.

다음 상황에서는 spawn보다 다른 행동을 우선할 수 있다.

- 정보 부족 → 사용자에게 질문
- 최신 사실 부족 → 허용된 source 조회
- 전문 자격 판단 필요 → 적절한 검토 권고
- 예산 부족 → 한계 설명 또는 판단 보류

```yaml
spawn_proposal:
  task_id: "dt_0023"
  role: "critic"
  purpose: "현재 설계의 장애 복구 취약점을 검토한다."
  target_uncertainty: "중복 실행이 상태 정합성을 깨뜨리는가?"
  expected_decision_impact: "복구 계약을 필수 요구사항으로 추가할 수 있다."
```

Control Plane은 budget, capability, concurrency 및 lifecycle policy를 검사한 뒤 생성한다.

---

# 14. Scheduler Contract

## 14.1 HEKATE의 판단

HEKATE는 다음을 제안한다.

- continue
- request information
- retrieve evidence
- spawn
- wait
- stop
- commit
- abstain

Continue에는 다음 정보가 필요하다.

```yaml
continuation_proposal:
  task_id: "dt_0023"
  unresolved_issue: "..."
  next_action: "..."
  expected_information_gain: "..."
  decision_impact: "..."
```

"한 번 더 생각하면 좋아질 것 같다"만으로 계속하지 않는다.

## 14.2 Control Plane의 Enforcement

Control Plane은 다음을 검사한다.

- task validity
- remaining budget
- deadline
- concurrency limit
- duplicate work
- allowed capability
- cancellation state
- maximum rounds
- input revision

Control Plane은 모델의 불확실성 판단을 사실로 인증하지 않는다.

## 14.3 Expected Value

`Expected benefit > expected cost`는 설계 원칙이다.

v0.1에서는 정밀한 수치 추정 대신 다음을 사용한다.

- 구체적인 미해결 문제
- 문제를 줄일 수 있는 실행 가능한 작업
- 결론에 영향을 줄 가능성
- bounded budget

---

# 15. Stopping과 Outcome

## 15.1 종료와 Commit은 별개다

Deliberation 종료는 계산을 중단하는 결정이다.

Position commit은 특정 판단을 채택하는 결정이다.

예산 소진은 Position 채택을 강제하지 않는다.

## 15.2 Autonomous Stop Signals

- 중요한 claim이 안정적이다.
- 새로운 독립 evidence가 추가되지 않는다.
- 다음 작업의 구체적 목적이 없다.
- 주요 반론이 해결되었거나 한계로 명시되었다.
- 추가 검토가 반복에 그친다.

수렴은 정확성의 증명이 아니다.

## 15.3 Forced Stop Conditions

- budget reservation 불가
- deadline reached
- maximum rounds reached
- user cancellation
- immediate answer request
- resource constraint
- permission revoked

Hard limit은 HEKATE가 무시할 수 없다.

## 15.4 Outcomes

```text
POSITION_COMMITTED
POSITION_RETAINED
PROVISIONAL_ANSWER
ABSTAINED
NEEDS_USER_INPUT
CANCELLED
FAILED
```

별도로 `stop_reason`을 기록한다.

```yaml
completion:
  outcome: "PROVISIONAL_ANSWER"
  stop_reason: "budget_exhausted"
  uncertainty:
    - "실제 workload 검증이 수행되지 않았다."
```

---

# 16. Budget Manager

## 16.1 Budget Scope

Task budget에는 다음을 포함한다.

- HEKATE planning
- subordinate inference
- HEKATE synthesis
- schema repair
- retries
- 유료 tools
- task 관련 background work

System-level budget도 별도로 적용한다.

## 16.2 Reservation Protocol

```text
Estimate bounded operation cost
→ Atomically reserve
→ Dispatch
→ Record usage
→ Settle
→ Release unused reservation
```

동시 요청은 atomic reservation으로 경쟁을 제어한다.

Task budget과 system budget을 모두 만족해야 dispatch할 수 있다.

## 16.3 Budget Ledger

```yaml
budget_reservation:
  id: "br_001"
  task_id: "dt_0023"
  attempt_id: "at_001"
  operation_id: "op_dispatch_001"
  status: "reserved"
  reserved_cost_usd: 0.08
  actual_cost_usd: null
  pricing_version: "..."
```

Final synthesis 또는 한계 설명에 필요한 budget을 별도로 남긴다.

## 16.4 Usage Uncertainty

Timeout이나 cancellation이 발생해도 외부 비용이 발생했을 수 있다.

Usage가 확인되지 않은 reservation은 즉시 0으로 정산하지 않는다.

`pending_settlement`로 유지하고 보수적으로 계산한 뒤 reconciliation한다.

## 16.5 Guarantee Boundary

다음은 구분한다.

- 새 작업의 dispatch 차단
- output token 등 설정 가능한 한도
- 이미 실행 중인 외부 호출의 실제 취소
- provider의 최종 청구 금액

외부 provider의 계량 지연이나 취소 제한까지 포함한 무조건적인 절대 비용 상한을 주장하지 않는다.

가능한 상한, 추정 오차 및 미정산 비용을 명시한다.

---

# 17. Agent Lifecycle

Agent lifecycle과 task lifecycle은 분리한다.

## 17.1 Agent States

```text
REQUESTED
CREATING
READY
BUSY
RETIRING
DELETE_PENDING
DELETED
FAILED
```

`DONE`은 작업 결과 상태이지 persistent agent의 영구 종료 상태가 아니다.

## 17.2 Spawn Flow

1. Spawn intent를 durable storage에 기록한다.
2. Policy 및 capability를 검증한다.
3. Budget을 예약한다.
4. Provider agent 생성을 요청한다.
5. Provider identifier를 registry에 연결한다.
6. Task를 dispatch한다.

## 17.3 Retirement Flow

1. 유효 결과 또는 실패 기록이 durable하게 저장되었는지 확인한다.
2. 진행 중인 execution과 dependency를 확인한다.
3. Agent를 `RETIRING`으로 전환한다.
4. Delete operation을 기록하고 provider deletion을 요청한다.
5. 삭제가 확인된 후 `DELETED`로 전환한다.

삭제 실패 시 `DELETE_PENDING`으로 유지하고 재시도한다.

Agent deletion은 모든 archive, backup 또는 외부 보존 데이터의 즉시 삭제를 의미하지 않는다.

데이터 보존·삭제 정책은 별도로 적용한다.

---

# 18. Registry

Registry는 subordinate memory를 복제하지 않는다.

```yaml
agent:
  registry_id: "ha_0042"
  creation_operation_id: "op_create_0042"

  provider:
    type: "letta"
    agent_id: "agent-xxxx"

  role: "critic"
  persistence: "ephemeral"

  lifecycle_state: "BUSY"
  active_attempt_id: "at_001"

  capability_profile: "reasoning-readonly"
  capability_policy_version: "1"

  created_at: "..."
  last_observed_at: "..."
  deletion_requested_at: null
```

Registry에는 intended state와 마지막 observed provider state를 구분해서 기록할 수 있다.

둘이 다르면 reconciliation 대상이다.

---

# 19. Delivery, Idempotency, and Recovery

## 19.1 Exactly-once를 기본 가정하지 않는다

외부 호출과 message delivery는 중복되거나 응답이 유실될 수 있다.

목표는 다음과 같다.

> 중복 전달을 허용하되, 동일한 논리적 결과와 상태 변경이 중복 적용되지 않도록 한다.

주요 operation에는 `operation_id`를 사용한다.

실행 재시도에는 새로운 `attempt_id`를 사용한다.

## 19.2 Ambiguous Creation

Provider 생성이 성공했는지 불명확하면 무조건 다시 생성하지 않는다.

가능한 경우:

- operation metadata 조회
- provider enumeration
- 기존 provider identifier 확인

으로 reconciliation한다.

확인이 불가능하면 unknown 상태로 남기고 escalation한다.

## 19.3 Late Results

취소되거나 timeout된 attempt의 늦은 결과는 audit용으로 저장할 수 있다.

그러나 자동으로 유효 결과나 Position commit 근거에 편입하지 않는다.

필요하면 새로운 유효 task에서 명시적으로 재평가한다.

## 19.4 Restart Recovery

Control Plane 재시작 시 다음을 확인한다.

- unfinished operations
- dispatched/running attempts
- unsettled budget reservations
- orphan provider agents
- pending deletions
- incomplete memory projections

Provider 조회 기능의 한계 때문에 자동 복구가 불가능한 항목은 운영자 확인 대상으로 남긴다.

## 19.5 Durable Work Queue

실행 의도 기록과 작업 유실 방지를 위해 durable queue 또는 transactional outbox를 사용할 수 있다.

v0.1에서는 단일 database-backed worker로 시작해도 된다.

---

# 20. Concurrency and Cancellation

## 20.1 HEKATE Turns

v0.1에서는 동일 HEKATE agent의 turn을 직렬화한다.

서로 독립적인 subordinate attempts는 정책상 허용된 범위에서 병렬 실행할 수 있다.

Position commit에는 직렬화와 별도로 version validation을 적용한다.

## 20.2 New User Input

새로운 사용자 입력이 기존 task의 premise나 constraints를 변경하면 `input_revision`을 증가시킨다.

이전 revision을 사용한 결과는 재평가 없이 현재 결론에 적용하지 않는다.

## 20.3 Cancellation

Cancellation은 다음을 수행한다.

1. Durable cancellation intent 기록
2. 신규 dispatch 차단
3. 가능한 provider cancellation 요청
4. 이후 결과의 자동 채택 차단
5. usage settlement 및 agent cleanup

외부 호출의 즉시 중단을 보장할 수 없다면 이를 숨기지 않는다.

---

# 21. Security Architecture

## 21.1 기본 원칙

- least privilege
- deny by default
- scoped access
- explicit capability grant
- mediated tool execution
- auditable mutation
- secret isolation
- bounded resources

## 21.2 보안 경계는 실행 지점에 존재해야 한다

Capability profile을 prompt나 metadata에 적는 것만으로는 보안이 성립하지 않는다.

검사는 실제 실행 지점에서 적용한다.

대상:

- Control Plane tool executor
- Letta-side tool execution
- filesystem access
- network egress
- shell/container
- MCP integration
- evidence retrieval
- credential use

Letta agent 객체의 분리는 OS-level isolation과 동일하지 않다.

## 21.3 Provider Access

모델 provider로 전달되는 prompt와 evidence도 외부 데이터 전송이다.

Tool network restriction과 model API 통신 경계를 구분한다.

민감 데이터 전달 정책은 model allocation 이전에 검사한다.

---

# 22. v0.1 Capability Profile

v0.1 subordinate의 기본 profile은 다음과 같다.

```yaml
capability_profile:
  name: "reasoning-readonly"
  version: "1"

  evidence_access:
    mode: "scoped"
    scope_from_task: true

  tools:
    allow:
      - "read_scoped_evidence"

  filesystem:
    mode: "none"

  external_network:
    mode: "deny"

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

Scoped evidence는 통제된 service를 통해 전달한다.

직접 filesystem이나 shell 접근이 없는 v0.1에서도 application-level capability enforcement는 필수다.

향후 code execution을 도입하면 container 또는 적절한 isolation boundary를 추가한다.

---

# 23. Untrusted Content and Secret Management

## 23.1 Untrusted Content

다음은 모두 untrusted content다.

- retrieved documents
- web pages
- user-provided artifacts
- subordinate outputs
- external tool results

이 내용이 tool permission이나 system policy를 변경할 수 없다.

Capsule과 evidence에는 source와 trust category를 구분해 유지한다.

## 23.2 Secrets

Subordinate prompt 또는 memory에 raw secret을 제공하지 않는다.

```text
Agent requests scoped operation
→ Policy check
→ Credential injection by trusted executor
→ Execution
→ Redacted result
```

Log와 error message에도 secret이 유출되지 않도록 redaction한다.

## 23.3 Mutation

외부 side effect는 Position과 별도 승인 경로를 가진다.

v0.1 Critic은 external mutation을 수행하지 않는다.

---

# 24. Context and Memory Strategy

```text
L0 Identity
L1 Active Task State
L2 Retrieved Long-Term State
L3 Archive
```

## L0 — Identity

- HEKATE identity
- user relationship
- epistemic principles

보안 정책의 요약을 포함할 수 있으나, enforcement 원본은 trusted policy configuration이다.

## L1 — Active Task State

- current objective
- input revision
- relevant Position version
- constraints
- immediate unresolved issues

## L2 — Retrieved Long-Term State

- historical Positions
- relevant prior conclusions
- project information
- agent result references

## L3 — Archive

- retained source documents
- raw transcripts
- logs
- artifacts

모든 장기 상태를 매 inference에 넣지 않는다.

중요한 structured state는 memory 요약만 믿지 않고 authoritative source에서 확인한다.

---

# 25. Model Allocation and Cost Optimization

## 25.1 v0.1 Model Allocation

v0.1은 고정된 HEKATE model과 Critic model 설정으로 시작한다.

동적 multi-model escalation은 필수가 아니다.

Model 설정에는 다음을 포함한다.

- allowed provider/model
- context limit
- output limit
- timeout
- data handling policy
- pricing version

## 25.2 Prompt Caching

가능하면 stable prefix와 volatile task content를 분리한다.

다만 실제 cache 사용 및 절감 효과는 provider와 Letta의 prompt 구성에 따라 달라진다.

측정 없이 cache hit를 가정하지 않는다.

## 25.3 Context Minimization

Task Capsule과 scoped evidence를 사용한다.

요약 과정에서 중요한 반론이나 조건이 누락되는지 평가한다.

## 25.4 Result Reuse

v0.1에서는 명시적으로 동일한 input 및 evidence version에 대한 재사용만 우선한다.

향후 semantic reuse를 도입하더라도 다음을 재검증한다.

- constraints
- evidence freshness
- access scope
- source availability
- model/policy compatibility

유사한 질문이라는 이유만으로 과거 결론을 현재 사실로 취급하지 않는다.

---

# 26. Background Deliberation

## 26.1 v0.1 Scope

자동 background deliberation은 v0.1 필수 구현에 포함하지 않는다.

v0.1에서는 unresolved task를 보존하고 명시적인 요청으로 재개할 수 있다.

## 26.2 Future Deliberative Sleep

Sleep은 응답 latency 제약이 낮은 background deliberation이다.

특정 memory consolidation 기능과 동일한 것으로 가정하지 않는다.

도입 조건:

- 별도 budget
- lower priority
- foreground 우선권
- durable scheduling
- cancellation support
- input revision validation
- Position version validation
- notification policy

## 26.3 Sleep Commit Policy

초기 Sleep 구현은 Position update candidate만 생성하는 것을 기본으로 한다.

실제 commit은 HEKATE의 유효한 serialized turn에서 최신 상태를 확인한 뒤 수행한다.

자동 commit을 허용하려면 topic별 정책을 추가한다.

새 evidence 없이 동일한 문제를 무기한 재검토하지 않도록 cooldown 및 재개 조건을 둔다.

---

# 27. Observability and Auditability

최소 기록 대상:

## Lifecycle

- create requested
- create observed
- dispatch
- result received
- retirement
- deletion requested
- deletion confirmed
- reconciliation

## Deliberation

- task created
- input revision changed
- continuation proposed
- continuation accepted/rejected
- result accepted/rejected
- stop reason
- outcome
- Position commit/conflict

## Cost

- reserved cost
- actual usage
- pending settlement
- input/output tokens
- cache usage when available
- model
- pricing version
- latency

## Security

- capability grant
- tool allow/deny
- scope validation
- mutation authorization
- secret redaction failure
- policy violation

각 event는 가능한 경우 다음 identifier를 포함한다.

- `task_id`
- `attempt_id`
- `operation_id`
- `agent_registry_id`
- `position_version`

LLM hidden reasoning을 보존하지 않는다.

구조화된 결론, evidence references, 결정 이유 및 실행 기록으로 provenance를 구성한다.

---

# 28. Failure Handling

## 28.1 Invalid Capsule

```text
Schema validation failure
→ Budget and deadline check
→ At most one repair request
→ Failure if still invalid
```

Repair 역시 task budget에 포함한다.

## 28.2 Agent Failure

Transient failure에만 제한된 retry를 적용한다.

권한 위반이나 잘못된 task 구성은 동일 요청을 반복하지 않는다.

Alternative model 실행은 새로운 attempt로 기록한다.

## 28.3 HEKATE Failure

Control Plane은 다음을 보존한다.

- task state
- attempts
- results
- budgets
- schedules
- registry
- Position history

HEKATE 복구 후 authoritative state를 조회하여 재개한다.

## 28.4 Storage Failure

필수 상태를 durable하게 기록할 수 없으면 신규 dispatch, commit 및 result deletion을 진행하지 않는다.

이미 실행 중인 작업은 복구 후 reconciliation한다.

## 28.5 Budget or Deadline Exhaustion

제한을 넘겨 reasoning을 계속하지 않는다.

가능하면 남겨둔 response budget으로 현재 결과와 한계를 설명한다.

Position commit이 불가능하면 기존 Position 유지 또는 판단 보류를 선택한다.

---

# 29. Architectural Invariants

### I-1
사용자가 관계를 맺는 외부 AI identity는 HEKATE 하나다.

### I-2
Position의 의미적 채택 제안은 HEKATE만 한다.

### I-3
Authoritative Position 저장은 Control Plane의 검증된 commit 경로로만 수행한다.

### I-4
Position은 versioned immutable history를 가진다.

### I-5
Stale input 또는 stale Position version에 기반한 commit은 자동 적용하지 않는다.

### I-6
Subordinate agent는 agent 생성·삭제 권한을 갖지 않는다.

### I-7
모든 agent 생성과 task dispatch는 Control Plane을 통과한다.

### I-8
모든 agent에는 실행 지점에서 강제되는 capability profile이 있다.

### I-9
Subordinate agent에게 전체 HEKATE context를 그대로 전달하지 않는다.

### I-10
유효한 완료 결과는 구조화된 Conclusion Capsule로 보존한다.

### I-11
필수 결과 또는 실패 기록을 durable하게 저장하기 전에 agent를 삭제하지 않는다.

### I-12
Agent memory, task state, registry 및 Position 원본을 혼합하지 않는다.

### I-13
비용이 발생하는 dispatch에는 유효한 budget reservation이 필요하다.

### I-14
Retry, repair 및 synthesis도 budget에 포함한다.

### I-15
취소된 attempt의 늦은 결과는 자동 commit에 사용하지 않는다.

### I-16
Deliberation 종료는 Position 채택을 강제하지 않는다.

### I-17
모델의 판단, 사용자의 실행 승인, 시스템의 권한 집행을 구분한다.

### I-18
동일 source의 반복 인용을 독립 evidence 증가로 취급하지 않는다.

---

# 30. v0.1 Required Scope

v0.1은 다음을 구현한다.

- persistent HEKATE
- ephemeral Critic
- Letta adapter
- durable Application Store
- Task / Conclusion Capsule
- Position versioning 및 atomic commit
- task / attempt / agent lifecycle 분리
- bounded scheduler
- budget reservation 및 settlement
- capability enforcement
- cancellation 및 late-result handling
- durable result collection
- deletion retry 및 reconciliation
- observability
- comparative evaluation

초기 정책:

- task당 Critic 최대 1개
- 추가 Critic review 최대 1회
- schema repair 최대 1회
- transient execution retry 최대 1회
- 모든 시도에 동일 task budget 적용
- 동일 HEKATE agent turn 직렬화

여기서 maximum review 횟수는 hard cap이다.

HEKATE는 더 일찍 종료하거나 Critic을 생성하지 않을 수 있다.

---

# 31. v0.1 Non-Goals

다음은 v0.1에서 구현하지 않는다.

- 자체 LLM
- 자체 general-purpose agent runtime
- 자체 vector database
- 자체 persistent memory engine
- 복잡한 고정 reasoning graph
- 모든 reasoning role
- recursive spawning
- persistent subordinate promotion
- autonomous code self-modification
- unrestricted shell execution
- 자동 background Position 변경
- 정교한 expected-value estimator
- calibrated Bayesian confidence engine
- semantic similarity만으로 수행하는 result reuse
- 복잡한 dynamic multi-model escalation
- 대규모 agent society

---

# 32. Implementation Plan

## Phase 0 — Integration Validation

검증:

- Letta agent 생성·조회·삭제
- tool execution 경계
- usage reporting
- timeout/cancellation 동작
- concurrent turn 동작
- provider metadata 및 reconciliation 가능성

완료 조건:

지원 기능, 미지원 기능 및 adapter 보완 범위가 문서화되어 있다.

## Phase 1 — Persistent HEKATE

구현:

- HEKATE 생성
- identity 및 memory
- 기본 대화
- turn serialization
- 기본 usage logging

완료 조건:

세션을 넘어 대화 연속성을 유지하고 재시작 후 정상적으로 재연결한다.

## Phase 2 — Safe Single Task Path

구현:

- Application Store
- task / attempt / registry
- Task / Conclusion Capsule
- budget reservation
- capability enforcement
- Critic 생성 및 dispatch
- durable result collection
- deletion

완료 조건:

HEKATE가 제한된 권한과 예산으로 Critic 하나를 실행하고 결과를 보존한 뒤 삭제할 수 있다.

## Phase 3 — Position and Recovery

구현:

- evidence metadata
- Position commit protocol
- version conflict handling
- cancellation
- retry deduplication
- deletion retry
- restart reconciliation

완료 조건:

Fault injection 상황에서 중복 commit과 stale commit이 차단되고, 복구 가능한 미완료 작업이 회수된다.

## Phase 4 — Bounded Deliberation

구현:

- continuation proposal
- deterministic limit enforcement
- stop reason
- non-commit outcomes
- final response budget

완료 조건:

구체적 목적 없이 reasoning을 반복하지 않으며 hard cap을 준수한다.

## Phase 5 — Comparative Evaluation

비교:

- HEKATE baseline
- HEKATE 단독 추가 추론
- HEKATE + Critic
- 더 강한 단일 모델

완료 조건:

비용·latency·품질 측면에서 Critic의 효용과 적용 대상이 식별된다.

## Phase 6 — Measured Expansion

평가 결과에 따라 다음 중 필요한 것만 추가한다.

- 추가 reasoning role
- multi-model routing
- semantic reuse
- background deliberation
- persistent specialists
- code execution isolation

---

# 33. Evaluation Protocol

## 33.1 Baselines

| 비교군 | 목적 |
|---|---|
| HEKATE 기본 응답 | 기본 품질·비용 |
| HEKATE 단독 추가 추론 | 추가 계산 자체의 효과 |
| HEKATE + Critic | 역할 분리 및 검토 효과 |
| 더 강한 단일 모델 | orchestration의 대안 |

비교는 가능한 한 동일 비용 조건과 동일 latency 조건에서 각각 수행한다.

## 33.2 Task Categories

- 단순 사실 질문
- 설계 검토
- 논리적 모순 탐지
- 불충분한 evidence
- 잘못된 사용자 전제
- 정답인 초기 결론
- 중요한 반론이 존재하는 문제
- 판단 보류가 적절한 문제

## 33.3 Quality Metrics

- correctness 또는 사전 정의된 rubric 점수
- critical error detection
- false objection rate
- 올바른 초기 결론의 잘못된 변경 비율
- unsupported claim rate
- 적절한 abstention
- evidence traceability

가능하면 평가자는 실행 방식을 모르는 상태로 결과를 평가한다.

## 33.4 Restraint and Independence

- unnecessary spawn rate
- 단순 요청의 비용 증가
- 사용자 주장만 변경했을 때 근거 없이 결론이 바뀌는 비율
- 동일 evidence에 대한 판단 일관성
- 반복 reasoning 발생률

## 33.5 Operational Metrics

- task completion rate
- latency distribution
- total cost
- reservation/settlement discrepancy
- orphan agent count
- deletion completion time
- duplicate result handling
- stale commit rejection
- permission violation blocking

## 33.6 Acceptance Thresholds

평가 전에 workload, budget 및 허용 latency에 맞춰 수치 기준을 정한다.

결과를 본 뒤 성공 기준을 유리하게 변경하지 않는다.

Multi-agent 경로가 비용 대비 개선을 보이지 않으면 비활성화하거나 적용 범위를 축소한다.

---

# 34. Required Fault-Injection Tests

v0.1 release 전 최소한 다음을 검증한다.

1. Agent 생성 응답 유실
2. 생성 직후 Control Plane 종료
3. 결과 수신 후 저장 전 장애
4. 결과 저장 후 삭제 전 장애
5. Delete API 실패
6. 동일 결과 중복 수신
7. Cancel 이후 늦은 결과 도착
8. Position version conflict
9. 사용자 입력 변경 후 stale result 도착
10. 병렬 budget reservation 경쟁
11. Usage 미확정 상태에서 timeout
12. Unauthorized tool request
13. Evidence scope 위반
14. Schema repair 실패
15. Authoritative storage unavailable

테스트 결과에는 다음을 기록한다.

- 기대 상태
- 실제 상태
- 자동 복구 여부
- 수동 조치 필요 여부
- 비용 및 데이터 손실 여부

---

# 35. Final Architecture

```text
                         USER
                           │
                           ▼
                 ┌───────────────────┐
                 │   HEKATE Agent    │
                 │ Persistent Letta  │
                 │                   │
                 │ Understanding     │
                 │ Judgment          │
                 │ Synthesis         │
                 │ Commit Proposal   │
                 └─────────┬─────────┘
                           │
                    Mediated requests
                           │
                           ▼
             ┌────────────────────────────┐
             │   HEKATE Control Plane     │
             │                            │
             │ Task / Attempt Management  │
             │ Scheduler / Stop Enforcer  │
             │ Budget Reservation         │
             │ Capability Enforcement     │
             │ Position Commit Validation │
             │ Lifecycle / Reconciliation │
             └──────┬─────────┬───────────┘
                    │         │
                    ▼         ▼
          ┌───────────────┐  ┌──────────────────┐
          │ Durable Store │  │  Letta Adapter   │
          │               │  └────────┬─────────┘
          │ Tasks         │           │
          │ Attempts      │           ▼
          │ Registry      │  ┌──────────────────┐
          │ Budget Ledger │  │   Letta Server   │
          │ Positions     │  │                  │
          │ Evidence Refs │  │ HEKATE           │
          │ Capsules      │  │ Ephemeral Critic │
          │ Audit Events  │  └──────────────────┘
          └───────┬───────┘
                  │
                  ▼
          ┌───────────────┐
          │ Scoped Archive│
          │ Sources / Logs│
          │ Artifacts     │
          └───────────────┘
```

모든 subordinate 결과는 Control Plane을 통해 검증·저장된 뒤 HEKATE에 전달된다.

---

# 36. Design Freeze Criteria

다음 원칙은 v0.1의 고정된 설계 방향이다.

- 단일 외부 identity
- LLM 판단과 deterministic enforcement의 분리
- Letta 기반 persistence
- 기본 단일 agent
- 선택적 ephemeral Critic
- versioned Position
- durable structured results
- bounded execution
- least privilege
- measurable improvement

다음 항목이 완료되기 전에는 구현 계약이 최종 동결되었다고 선언하지 않는다.

1. Letta integration capability matrix
2. State ownership definition
3. Capsule 및 commit schemas
4. Lifecycle transition rules
5. Budget accounting rules
6. Security enforcement map
7. Recovery behavior
8. Evaluation thresholds

---

# 37. 최종 정의

HEKATE는 새로운 지능 모델을 만드는 시스템이 아니다.

HEKATE는 persistent agent와 LLM의 추론 능력을 제한된 맥락, 권한, 예산 안에서 배치하고 평가하는 orchestration system이다.

HEKATE Agent는 판단한다.

Control Plane은 검증하고 집행한다.

Letta는 agent persistence와 execution substrate를 제공한다.

Application Store는 시스템의 authoritative operational state와 Position history를 보존한다.

Subordinate agent는 필요할 때 생성되는 bounded cognitive resource다.

Position은 HEKATE가 채택한 수정 가능한 판단이며, 모든 agent의 합의나 절대적 진리를 의미하지 않는다.

Deliberation 종료와 Position 채택은 별개다.

추가 계산이 정당화되지 않으면 중단하고, 근거가 부족하면 판단을 보류한다.

v0.1의 성공은 agent 수나 아키텍처의 복잡성이 아니다.

> 실제 사용에서 더 나은 판단을 제공하면서도, 비용·권한·상태·수명주기를 예측 가능하게 통제할 수 있는가가 성공 기준이다.

