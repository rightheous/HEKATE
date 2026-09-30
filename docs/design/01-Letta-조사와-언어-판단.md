# HEKATE v0.1 — Letta 조사와 구현 언어 결정

조사일: 2026-09-29 · 대상: 첨부된 HEKATE v0.1 Architecture Specification 전체 37절

## 1. 결정

**Rust와 Python 중에서는 Python을 선택한다.** HEKATE의 업무 규칙, 상태 전이, 정책, 비용 원장, Position commit, 복구, 평가를 Python으로 구현한다. 최신 Letta 연결은 공식 TypeScript Agent SDK를 사용하는 작은 bridge로 격리한다.

권장 구성은 **Python Control Plane + PostgreSQL + TypeScript Letta bridge + 고정 버전의 self-hosted Letta App Server**다. Letta backend는 첫 구현에서 `local` 하나로 제한한다. SDK에서 self-hosted 서버에 접속하는 방식은 `remote`이며, 서버의 상태 저장 방식인 `local`과 다른 축이다.

이것은 성능 벤치마크 결과가 아니라 **개인용, 소규모 동시성, 변경이 잦은 v0.1**이라는 명세에 대한 공학적 판단이다. 개발자의 두 언어 숙련도가 비슷하다고 가정했다. 매우 작은 메모리 한도, 강한 CPU 처리 요구, Rust 중심 운영 조직이 확인되면 판단을 재검토한다.

**중요한 조건:** 현재 공개 SDK의 옵션을 조합하는 것만으로 원 명세의 모든 hard cap을 보장했다고 선언해서는 안 된다. 런타임 내부 재시도와 compaction까지 포함한 실행·비용 제어를 Phase 0에서 검증해야 한다. 연결만 되는 것과 명세를 만족하는 것은 다르다.

## 2. 기존 Letta 지식에서 수정해야 하는 사실

현재 `letta-ai/letta`의 기본 브랜치는 현행 구현을 `letta-ai/letta-code`로 안내한다. 예전 V1 API 서버는 `archive` 브랜치에 남아 있다. 새 프로젝트의 기본값을 예전 Python 서버로 잡을 근거는 약하다. [공식 저장소](https://github.com/letta-ai/letta)

공식 `letta-python`은 deprecated이고, 새 기능을 받지 않는다고 명시한다. 따라서 `pip install letta-client`와 `AsyncLetta`를 새 HEKATE의 장기 통합 기반으로 제안하지 않는다. [Python SDK의 고정 커밋 README](https://github.com/letta-ai/letta-python/blob/3731731b18af66f7aa79001796b861d5829bae1a/README.md#L1-L15), [공식 Python 문서](https://docs.letta.com/api/python)

현행 공식 Agent SDK는 TypeScript이며 cloud/local/remote 연결을 제공한다. agent, conversation, session은 별도 개념이다. 지속해야 하는 것은 provider agent ID와 conversation ID이지 연결 객체가 아니다. [SDK README](https://github.com/letta-ai/letta-agent-sdk/blob/11cdd257ee68db6acb0a5f8025142f3add1021cc/README.md), [session 설명](https://docs.letta.com/agent-sdk/sessions)

이 변화는 원 명세의 핵심 철학을 무효화하지 않는다. 다만 “Letta adapter”의 실제 대상과 검증 계약을 바꾼다.

## 3. 조사 범위와 재현 가능한 기준점

공개 저장소 세 개를 실제로 내려받아 코드를 읽었다. Letta 자체의 이전 서버에 대해서는 공식 이동 안내를 확인했고, retired 서버 전체를 분석하지는 않았다.

| 저장소 | 조사한 commit | 소스에 적힌 버전 | 용도 |
|---|---|---|---|
| `letta-ai/letta-code` | `2b9588980d2ca8a6c678ad2b3748f83f6d7b8332` | `0.33.6` | 현행 runtime, App Server, local store, tool 경계 |
| `letta-ai/letta-agent-sdk` | `11cdd257ee68db6acb0a5f8025142f3add1021cc` | `0.8.23` | 연결·관리·stream·structured output |
| `letta-ai/letta-python` | `3731731b18af66f7aa79001796b861d5829bae1a` | 버전보다 deprecation 상태를 확인 | 과거 Python 연동의 적합성 판단 |

버전 번호는 조사한 소스의 `package.json` 값이며, 이 문서가 npm 배포물의 동일성이나 두 패키지 조합의 실행 호환성을 인증하지는 않는다. 설치 시에는 exact version, lockfile, package integrity, 실제 서버 SHA를 함께 고정해야 한다. SDK 소스는 `letta-code: 0.33.6` 의존성을 선언한다. [runtime package](https://github.com/letta-ai/letta-code/blob/2b9588980d2ca8a6c678ad2b3748f83f6d7b8332/package.json), [SDK package](https://github.com/letta-ai/letta-agent-sdk/blob/11cdd257ee68db6acb0a5f8025142f3add1021cc/package.json)

실제 LLM 호출, provider 청구 확인, 서버 기동, fault injection, 성능 측정은 수행하지 않았다. 아래의 “코드 확인”은 해당 코드 경로의 존재와 정적 동작을 확인했다는 뜻이다.

## 4. Letta가 맡는 부분

Letta는 상태가 지속되는 agent의 실행 기반이다. agent identity와 conversation을 유지하고, MemFS로 memory를 관리하며, 모델 실행과 tool 연계를 제공한다. agent가 실행되는 backend에 따라 저장·도구 실행 위치가 달라진다. [현행 SDK](https://github.com/letta-ai/letta-agent-sdk/blob/11cdd257ee68db6acb0a5f8025142f3add1021cc/README.md), [MemFS](https://docs.letta.com/concepts/memfs)

HEKATE는 이 위에 별도의 업무적 의미를 얹는다. Letta의 agent memory는 Position의 원본이 아니고, Letta run의 성공은 HEKATE task의 성공과 같지 않다. 실행이 끝났어도 evidence 검증, revision 검사, 비용 정산, retirement가 남는다. 이 구분은 원 명세 그대로 유지한다.

Letta에 있는 subagent, schedule, dreaming 기능을 HEKATE의 scheduler 대신 사용하지 않는다. v0.1에서는 HEKATE Control Plane만 Critic 실행을 승인하며, 자동 dreaming·schedule·recursive spawning은 꺼 둔다. 이는 기능 중복을 줄이는 것과 동시에 I-6, I-7, I-13을 지키기 위한 선택이다.

## 5. 실제 코드에서 발견한 설계상 중요 사항

### 5.1 생성 옵션과 실행 옵션은 다르다

SDK 타입에는 `CreateAgentOptions.allowedTools` 등이 보이지만, App Server용 `createAgentBody()`는 `allowedTools/disallowedTools`, `canUseTool`, 일부 dreaming override를 거절한다. 타입에 필드가 있다는 이유만으로 해당 backend에서 지원된다고 간주할 수 없다. [검증 구현](https://github.com/letta-ai/letta-agent-sdk/blob/11cdd257ee68db6acb0a5f8025142f3add1021cc/src/agent-creation.ts#L25-L49)

**설계 반영:** 생성은 system prompt, model, tags, `baseTools: []`, memfs 설정을 담당한다. 실행 권한은 매 session 재연결 때 다시 설정하고 실제 노출 tool 목록을 검사한다. 잘못된 설정이면 inference 전에 종료한다.

### 5.2 서버 tool과 client tool을 모두 통제해야 한다

생성 옵션의 `baseTools`를 생략하면 기본 서버 tool이 붙을 수 있으며, client-side tool은 별도로 노출된다. `allowedTools`는 실행 때의 client tool 목록을 제한한다. [두 옵션의 계약](https://github.com/letta-ai/letta-agent-sdk/blob/11cdd257ee68db6acb0a5f8025142f3add1021cc/src/types.ts#L930-L947)

**설계 반영:** Critic은 `baseTools: []`, 정확한 client allowlist, `strict`, 실행 함수 내부의 재검사를 함께 사용한다. 허용 이름은 `read_scoped_evidence`와 결과 제출용 내부 채널뿐이다. callback 하나를 모든 실행 지점의 보안 경계라고 가정하지 않는다.

### 5.3 structured output 기본 재시도가 HEKATE 예산 정책을 우회할 수 있다

`streamStructuredTurns()`의 기본 `maxRetries`는 2다. portable 경로는 `StructuredOutput` tool을 추가하고 성공적인 tool 결과 뒤 abort를 요청한다. JSON schema validation은 도메인 권한 검증도, 사실 검증도 아니다. [structured output loop](https://github.com/letta-ai/letta-agent-sdk/blob/11cdd257ee68db6acb0a5f8025142f3add1021cc/src/structured-output-session.ts#L45-L96), [tool validator](https://github.com/letta-ai/letta-agent-sdk/blob/11cdd257ee68db6acb0a5f8025142f3add1021cc/src/structured-output.ts)

**설계 반영:** SDK 자동 repair는 `maxRetries: 0`으로 끈다. HEKATE가 budget을 예약하고 새 `attempt_id`를 발급한 repair만 최대 한 번 수행한다. portable submission이 추가 유료 호출을 유발하지 않도록 terminal 처리까지 시험한다.

### 5.4 runtime 자체에도 추가 재시도가 있다

listener에는 LLM API error와 empty response에 대한 별도 retry 한도가 있으며, `shouldRetryPostStopTurn()` 같은 경로가 이를 사용한다. 따라서 SDK structured-output retry를 꺼도 모든 자동 재시도가 꺼진 것은 아니다. [상수](https://github.com/letta-ai/letta-code/blob/2b9588980d2ca8a6c678ad2b3748f83f6d7b8332/src/websocket/listener/constants.ts#L58-L63), [재시도 판단](https://github.com/letta-ai/letta-code/blob/2b9588980d2ca8a6c678ad2b3748f83f6d7b8332/src/websocket/listener/turn-send.ts#L133-L153)

**설계 반영:** 새로운 inference를 일으키는 재시도는 Control Plane이 소유한다. 새 inference 없는 stream 재접속은 별도 transport 복구로 분류한다. 기존 runtime에서 이를 정확히 분리·제한할 수 없으면 작은 runtime 변경이 필요하며, 그 전에는 전체 명세 준수로 출시하지 않는다.

### 5.5 `max_turns`는 존재하지만 과금 호출 수와 동일하지 않다

`RuntimeExecutionSettings.max_turns`와 listener의 step-count 검사가 존재한다. 그러나 확인한 검사는 stream을 처리한 뒤 수행되고 `requires_approval` 경계 등을 별도로 다룬다. 이 하나만으로 모든 모델 요청 전에 동일한 hard cap이 적용된다고 증명할 수 없다. [설정](https://github.com/letta-ai/letta-code/blob/2b9588980d2ca8a6c678ad2b3748f83f6d7b8332/src/runtime-execution-settings.ts), [검사 지점](https://github.com/letta-ai/letta-code/blob/2b9588980d2ca8a6c678ad2b3748f83f6d7b8332/src/websocket/listener/turn.ts#L315-L367)

**설계 반영:** `max_turns`는 방어 수단 하나로만 쓴다. 모든 billable call의 pre-dispatch guard와 token/model 설정 검증을 별도 계약으로 둔다. 현재 SDK의 상위 session 옵션에 이를 임의의 이름으로 넣지 않는다.

### 5.6 compaction도 모델 호출을 한다

local compaction의 `runGenerateText()`는 모델을 호출하고, context overflow 시 축약된 입력으로 다시 시도하는 경로가 있다. “사용자 메시지 한 번 = 유료 호출 한 번”이라는 예산 모델은 맞지 않는다. [compaction 구현](https://github.com/letta-ai/letta-code/blob/2b9588980d2ca8a6c678ad2b3748f83f6d7b8332/src/backend/local/compaction.ts#L405-L549)

**설계 반영:** planning, Critic, synthesis, repair 외에 runtime compaction·memory 관련 유료 실행도 같은 task budget에 귀속한다. 귀속되지 않은 background inference는 기본 거절한다.

### 5.7 숨김 agent가 reconciliation을 방해할 수 있다

조사한 local store의 `listAgents()`는 hidden agent를 먼저 제외한 다음 tag/query filter를 적용한다. ID를 잃은 hidden Critic을 tag로 찾는 설계는 이 코드 경로에서 성립하지 않는다. [local enumeration](https://github.com/letta-ai/letta-code/blob/2b9588980d2ca8a6c678ad2b3748f83f6d7b8332/src/backend/local/local-store.ts#L439-L479)

**설계 반영:** 전용 Letta 인스턴스의 Critic은 `hidden: false`로 만들고 HEKATE UI가 노출하지 않는다. `hekate-owner:<deployment-id>`와 `hekate-create:<operation-id>` tags로 찾는다. 이름이나 tag는 provider의 unique constraint가 아니므로 중복 생성 방지를 대신하지 못한다.

### 5.8 메시지 중복 제거에는 수명과 범위가 있다

OTID/client message ID로 같은 입력을 재식별할 수 있다. 하지만 조사한 listener의 accepted-input 중복 제거는 runtime의 bounded map을 사용한다. 재시작을 통과하는 전역 exactly-once 보장으로 확대 해석해서는 안 된다. [SDK OTID](https://github.com/letta-ai/letta-agent-sdk/blob/11cdd257ee68db6acb0a5f8025142f3add1021cc/src/types.ts#L125-L139), [map 기반 처리](https://github.com/letta-ai/letta-code/blob/2b9588980d2ca8a6c678ad2b3748f83f6d7b8332/src/websocket/listener/inbound-dispatch.ts#L23-L49)

**설계 반영:** PostgreSQL operation journal, outbox, result inbox가 필수다. 응답 유실 후에는 transcript·runtime·operation tag로 조회하고, 실행 여부를 확정하지 못하면 `UNKNOWN`으로 보존한다.

### 5.9 취소와 session close는 다르다

`abort()`는 취소 요청이고 terminal result까지 stream을 계속 읽어야 한다. 연결 종료 후에는 새 session을 열어야 한다. 외부 tool 등록도 연결에 귀속되므로 재연결 시 복원해야 한다. [session lifecycle](https://docs.letta.com/agent-sdk/sessions), [external tool lifecycle](https://docs.letta.com/self-hosting/app-server/external-tools)

**설계 반영:** 취소 즉시 새 dispatch와 결과 채택은 막되, provider 중단과 청구 확정은 별도로 추적한다. `close()` 반환을 취소 완료 증거로 쓰지 않는다.

### 5.10 usage 필드가 항상 확정 비용을 뜻하지 않는다

SDK result의 `totalCostUsd`는 optional이다. local provider executor에는 input/output/cache/reasoning usage를 stream으로 바꾸는 코드가 있다. 모든 provider·compaction·실패 경로의 과금이 완전하게 집계된다는 보장은 추가 검증이 필요하다. [result 타입](https://github.com/letta-ai/letta-agent-sdk/blob/11cdd257ee68db6acb0a5f8025142f3add1021cc/src/types.ts#L1065-L1090), [usage 변환](https://github.com/letta-ai/letta-code/blob/2b9588980d2ca8a6c678ad2b3748f83f6d7b8332/src/backend/dev/provider-turn-executor.ts#L303-L344)

**설계 반영:** null을 0원으로 바꾸지 않는다. provider별 가격표와 usage completeness를 함께 보존하며, 미확정 비용은 `pending_settlement`로 남긴다.

## 6. HEKATE 요구사항과 Letta 지원의 대응

`확인`은 정적 코드/문서 확인, `조건부`는 backend·버전·fault test가 필요한 상태, `HEKATE`는 자체 구현 책임을 뜻한다.

| 요구 | 판정 | 통합 방식 및 제한 |
|---|---|---|
| agent 생성·조회·삭제 | 확인 | SDK create + agents retrieve/delete. 삭제 후 조회·목록으로 확인 |
| persistent identity/conversation | 확인 | agent/conversation ID 저장, 재시작 후 resume |
| MemFS | 확인 | HEKATE 사용, Critic은 `memfs:false`; Position은 DB 원본 |
| agent enumeration | 조건부 | local hidden 제외 주의, owned tags와 pagination 시험 |
| 서버/외부 tool 경계 | 확인/조건부 | baseTools 비우기 + session allowlist + 실제 executor 검사 |
| 비동기 실행 | 확인 | async send/stream. 업무 상태·durable queue는 HEKATE |
| 취소 | 조건부 | abort 가능. provider 실제 중단·잔여 청구는 보장 범위 확인 |
| usage | 조건부 | optional usage, 실패·compaction 포함 완전성 검증 |
| turn 동시성 | 조건부 | provider queue에 의존하지 않고 HEKATE agent 단위 직렬화 |
| 모델 설정 | 확인/조건부 | fixed model 사용, output limit의 실제 provider 전달 검증 |
| 재시작 후 event replay | 조건부 | transcript/runtime 조회로 대조; 누락 이벤트 완전 복원 가정 금지 |
| agent 생성 idempotency | 미확인 | tags는 조회 단서. journal+UNKNOWN reconciliation 필요 |
| task·system budget atomic reserve | HEKATE | PostgreSQL transaction |
| revision·Position CAS | HEKATE | DB transaction, authenticated actor binding |
| hard call budget 및 내부 retry 통제 | 조건부/보완 필요 | runtime guard integration이 release gate |
| evidence scope·retention | HEKATE | metadata와 archive 접근 서비스 |
| 검증된 capsule 보존·late result 분리 | HEKATE | result inbox + typed result store |

관리 메서드의 구현은 [App Server management transport](https://github.com/letta-ai/letta-agent-sdk/blob/11cdd257ee68db6acb0a5f8025142f3add1021cc/src/app-server-management.ts#L75-L183)에서 확인했다. 구체적인 시험 기준은 [통합 검증 설계](03-통합검증과-구현순서.md)에 있다.

## 7. Rust와 Python 비교

| 판단 축 | Python | Rust | HEKATE v0.1 판단 |
|---|---|---|---|
| 최신 Letta 공식 SDK | 현행 Python SDK 없음 | 조사한 공식 current SDK는 TS | 양쪽 모두 bridge/직접 protocol 통합 필요 |
| 빠른 schema·policy 수정 | Pydantic, 작은 async service로 표현 가능 | enum·newtype로 강한 정적 모델 가능 | 변경량이 많은 초기 단계는 Python 우선 |
| 동시성 | async I/O에 적합, 잘못된 blocking 작업 주의 | 정적 ownership, 효율적 실행 | 현재 workload에는 양쪽 모두 가능 |
| 상태 정합성 | DB transaction과 제약 필요 | DB transaction과 제약 필요 | 언어가 remote 중복·stale commit을 해결하지 않음 |
| 메모리/CPU 효율 | 일반적으로 더 큰 runtime 비용 | 낮은 overhead를 기대할 수 있음 | 현재 성능 요구·측정으로 우열이 결정되지 않음 |
| 평가·실험·데이터 가공 | 같은 언어로 평가·분석 구성 쉬움 | 별도 Python 도구를 병행할 가능성 | 본 명세의 comparative evaluation에 Python 유리 |
| 배포 | Python + Node + DB/runtime | Rust binary + Node + DB/runtime | Rust여도 Letta 전체가 단일 binary가 되지 않음 |
| 타입 안전성 | strict typing + runtime validation + DB constraints | 더 강한 compile-time 보장 | Rust의 실질적 장점이나 현재 최우선 비용은 아님 |

Python의 async I/O 적합성과 Rust의 ownership/concurrency 장점은 각각 공식 문서에서 확인할 수 있다. 위 표의 개발 비용과 HEKATE 적합도는 해당 특성과 명세를 결합한 **설계 판단**이다. [Python asyncio](https://docs.python.org/3/library/asyncio.html), [Rust concurrency](https://doc.rust-lang.org/book/ch16-00-concurrency.html)

### 왜 Python인가

1. 핵심 작업이 LLM·DB·stream·tool I/O와 업무 규칙이며, CPU 병목은 측정되지 않았다.
2. 위험한 오류는 process 안의 자료구조보다 외부 호출과 DB 사이의 실패, revision, 권한, 과금 경계에서 발생한다.
3. scheduler·capsule·평가 기준은 v0.1에서 자주 바뀔 가능성이 높다. domain rule을 짧고 읽기 쉬운 코드로 고정하고 fault test에 투자하는 편이 적합하다.
4. Rust를 선택해도 Letta protocol의 변화와 보완 작업은 그대로 남는다.

### 언제 Rust로 바꿀 것인가

Control Plane 자체의 CPU/RSS/대기열 지연이 목표를 넘고, LLM·DB 시간을 제외한 프로파일링에서 병목이 확인된 경우다. 우선 해당 executor·proxy·parser만 별도 Rust service로 교체한다. 처음부터 Python/Rust 이중 domain 구현은 하지 않는다.

Rust 경험이 훨씬 많은 팀이라면 개발 속도에 대한 가정이 바뀌므로 Rust Control Plane도 타당하다. 언어가 아니라 같은 DB/bridge 계약과 불변식을 지켜야 한다.

### TypeScript 전체 구현은 어떤가

선택 범위를 넓힌다면 TypeScript 단일 Control Plane도 유력하다. 현행 공식 SDK와 언어를 맞춰 bridge를 없앨 수 있기 때문이다. 다만 사용자가 비교를 요청한 Rust/Python 사이에서는 Python이 우선이다. “Python이 모든 언어보다 항상 우수하다”는 결론은 아니다.

## 8. 최종 설계에 반영한 명세 보충

다음은 첨부 명세를 대체하는 변경이 아니라 모호한 구현 경계를 구체화한 제안이다.

| 항목 | 구체화 |
|---|---|
| Letta 기준선 | 현행 App Server + local backend, source/SDK exact pin |
| unknown 상태 | task/attempt enum을 늘리는 대신 operation observation을 `UNKNOWN`으로 별도 기록 |
| Critic 수 | v0.1에서는 task당 provider Critic 객체 최대 하나. retry는 동일 객체의 새 conversation에서 실행 |
| 추가 review | 최초 1회 + 추가 1회 = 최대 2 review rounds |
| repair/retry | task 전체에서 schema repair 1회, execution retry 1회. round마다 초기화하지 않음 |
| root 과금 | HEKATE planning/synthesis/final response도 execution attempt와 budget 대상 |
| role 권한 | JSON의 `agent_id`를 신뢰하지 않고 runtime binding으로 actor 도출 |
| runtime 자동 기능 | skills, hooks, mods, schedules, dreaming, native subagents를 기본 비활성화 |
| provider 호출 | 정상 inference와 compaction을 포함해 모두 사전 허가된 bounded envelope에 포함 |
| memory projection | DB commit 이후 비동기 갱신, authoritative source 및 version을 함께 투영 |

**원 명세의 design freeze 조건은 아직 충족되지 않았다.** 조사와 아래 구현 설계는 완료했지만, 실제 배포 조합에서 통합 검증을 통과하고 평가 임계치를 사전 고정해야 실행 계약을 동결할 수 있다.

다음 문서: [디렉토리·파일·함수 설계](02-구현-상세설계.md), [통합 검증·구현 순서](03-통합검증과-구현순서.md).
