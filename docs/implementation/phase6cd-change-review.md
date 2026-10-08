# HEKATE Phase 6C·6D 변경 정리

검토 시각: 2026-10-06 20:01:37 UTC

핵심 엔진의 구현은 대부분 완료됐다. 실제 Qwen의 단일 Task 응답 채택까지 확인했지만, 일반 실행 구성과 일부 제품·운영 인터페이스는 남아 있다. 전체 v0.1 출시 완료로 표현하지 않는다.

## 기준

- Worktree: `/home/hekate/hekate-phase6c-ollama-qwen-profile`
- Branch: `phase6c-ollama-qwen-profile`
- HEAD: `9c25acca594d3edb5db4f2ce710e490b7e6d95b7`
- 검토 전: 수정 17개 + 미추적 200개 = 217개 파일
- Stage는 비어 있다. Commit·push·PR·merge는 수행하지 않았다.
- Phase 4~6B는 현재 HEAD의 커밋 이력에 이미 포함된다. 이번 미커밋 변경은 Phase 6C·6D에 집중된다.
- `main...HEAD`에는 main 전용 1개, 현재 브랜치 전용 19개 커밋이 있다. 작업 브랜치의 완료와 main 통합을 구분한다.

## 완료한 핵심과 남은 범위

| 영역 | 현재 확인된 범위 |
|---|---|
| 계약·DB·예산·permit·outbox/inbox | 이전 단계에서 구현·검증됨 |
| Evidence·Position·이력·권한·만료 | 이전 단계에서 구현·fake-runtime 검증됨 |
| Critic·synthesis·bounded deliberation | 이전 단계에서 구현·fake-runtime 검증됨 |
| Letta memory projection | Phase 6A 구현 있음. 이번 실제 Qwen Task에서는 실행하지 않음 |
| 최종 요청 토큰 측정·Qwen renderer/tokenizer | 구현됨. 이번 실제 입력 4,086 tokens가 provider usage와 일치 |
| 실제 Qwen 응답 | Phase 6D 성공: 한 번의 생성, strict 계약·binding 통과, DB 채택, replay 효과 불변 |
| 일반 실행 구성 | `config/models.yaml`은 미설정, 예산·deadline 기본값은 미설정, `deploy/compose.yaml`은 비어 있음 |
| HTTP API·인증 | `src/hekate/api`에 NotImplementedError 함수가 남아 있음 |
| 운영자 복구·평가 | `application/recovery.py`, `evaluation`에 미구현 함수가 남아 있음 |
| 별도 scheduler·policy·tool 인터페이스 | 미구현 함수 있음. worker·turns·workflow의 기존 실제 경로와 구분해 출시 범위를 확인해야 함 |
| G7/G8·production | 동일 실행 resume는 미지원. production은 blocked. 한 실제 요청의 일치를 모든 요청의 승인으로 확대하지 않음 |

단순 `pass` 검색 결과는 미구현으로 계산하지 않았다. 예외 타입 선언과 정상 예외 정리도 포함되기 때문이다. 미구현 인터페이스가 존재한다고 이미 구현된 다른 실행 경로까지 미구현으로 단정하지 않았다.

## 제출 단위

현재 변경을 하나의 기능 커밋으로 검토하는 것을 권장한다.

권장 제목: `feat: integrate local Qwen and complete validated turn execution`

| 구분 | 파일 수 | 처리 |
|---|---:|---|
| 구현·설정·계약·fixture·테스트·문서 | 29 | 제출 후보 |
| 핵심 성공/회귀/timeout 진단 근거 | 21 | 제출 후보 |
| 기타 진단·중간 캡처·승인 ledger | 167 | 로컬 보존, 기본 제출 제외 |
| 이번 검토 문서·manifest | 2 | 정리 산출물, 별도 포함 가능 |

검토 전 후보 50개는 약 17.1 MB, 로컬 보존 167개는 약 40.8 MB다. 7.96 MB tokenizer asset은 설치된 모델의 tokenizer 재현에 필요한 데이터이며 모델 weight는 아니다. 원 모델 GGUF나 가중치는 포함하지 않는다.

이 목록은 선택적인 제출안이다. 원 Phase 6D 문서의 일부 과거 진단 경로는 로컬 보존 자료를 가리키므로, 해당 링크가 모두 새 clone에 포함된다고 가정하지 않는다. 과거 실패·UNKNOWN 기록을 삭제하거나 성공으로 바꾸지 않는다.

## 변경 내용

- 설치된 Qwen tokenizer·renderer·정규화와 패키지 asset 포함
- local candidate/profile 및 기존 Task 실행 설정 연결
- 서버 생성 native JSON Schema 적용, SDK portable outputFormat의 모델별 분리
- 최종 요청 measurement·profile·permit digest 일치 검사
- 출력 경로의 test-only 관측, 공개 산술 캡처와 재현 probe
- 느린 로컬 생성에서 SDK turn timeout이 먼저 수집을 끝내는 문제의 진단과 대기 설정 보정
- 실제 응답 채택·usage 정산·재시작 replay 근거 보존

## 이번 정리의 확인

- 제출 후보 50개에서 지정한 민감정보 패턴 발견 0건
- 제출 후보 JSON 형식 오류 0건
- `git diff --check` 통과
- runtime patch 실제 SHA256과 versions.lock 일치:
  `70e395632b9899edd213a4bf2e52acaa0662443f954217c0eb6271461db4be05`
- 전체 secret audit 또는 일반 production 검증을 수행했다는 의미는 아니다.
- 이번 정리에서 runtime 테스트·실제 inference·DB 변경은 실행하지 않았다.
- 직전 Phase 6D 성공 artifact의 최종 파일 해시 30개 일치는 앞선 검토에서 확인했다.

## 제출 후보 — 구현·문서·fixture

- `bridge/letta/src/main.ts`
- `bridge/letta/src/protocol.ts`
- `contracts/generated/bridge.v1.schema.json`
- `integration/letta/patches/provider-call-context-usage.patch`
- `integration/letta/versions.lock.json`
- `pyproject.toml`
- `scripts/phase3_runtime_probe.py`
- `src/hekate/application/lifecycle.py`
- `src/hekate/application/operations.py`
- `src/hekate/application/turns.py`
- `src/hekate/domain/bridge_contracts.py`
- `src/hekate/domain/models.py`
- `src/hekate/infrastructure/letta/adapter.py`
- `src/hekate/infrastructure/letta/provider_gateway.py`
- `src/hekate/infrastructure/letta/token_accounting.py`
- `src/hekate/ports/runtime.py`
- `src/hekate/settings.py`
- `config/local-candidates.yaml`
- `docs/implementation/phase6c-ollama-qwen-profile.md`
- `docs/implementation/phase6d-local-qwen-smoke.md`
- `integration/runtime/fixtures/qwen35-renderer-tokenizer-v1.json`
- `scripts/build_qwen35_unicode_flags.py`
- `scripts/phase6c_ollama_qwen_probe.py`
- `src/hekate/infrastructure/letta/assets/qwen35_installed_tokenizer.v1.json`
- `src/hekate/infrastructure/letta/assets/qwen35_unicode_flags.v1.json`
- `src/hekate/infrastructure/letta/qwen_ollama.py`
- `tests/unit/test_phase6d_local_probe_guard.py`
- `tests/unit/test_provider_gateway_sse.py`
- `tests/unit/test_qwen_ollama.py`

## 제출 후보 — 검증 근거

- `integration/runtime/artifacts/p6b-legacy-c469d211.json`
- `integration/runtime/artifacts/p6c-20261005T200837Z-60478a24-ollama-qwen-runtime.json`
- `integration/runtime/artifacts/p6c-permit-guard-20261005T201406Z-ce684841.json`
- `integration/runtime/artifacts/p6d-20261006T190724Z-ab1a653c-output-observed-qwen-capture/provider-choice-0.txt`
- `integration/runtime/artifacts/p6d-20261006T190724Z-ab1a653c-output-observed-qwen-capture/provider-response.sse`
- `integration/runtime/artifacts/p6d-20261006T192628Z-27d0c82c-output-delivery.json`
- `integration/runtime/artifacts/p6d-20261006T192911Z-57507dd1-captured-sse-30000ms.json`
- `integration/runtime/artifacts/p6d-20261006T193011Z-57b83bb2-captured-sse-240000ms.json`
- `integration/runtime/artifacts/p6d-20261006T193535Z-9c58f9a2-qwen-output-diagnosis.json`
- `integration/runtime/artifacts/p6d-20261006T193948Z-8bc886ed-output-observed-qwen-followup-capture/adapter-observations.jsonl`
- `integration/runtime/artifacts/p6d-20261006T193948Z-8bc886ed-output-observed-qwen-followup-capture/app-server-assistant.txt`
- `integration/runtime/artifacts/p6d-20261006T193948Z-8bc886ed-output-observed-qwen-followup-capture/bridge-observations.jsonl`
- `integration/runtime/artifacts/p6d-20261006T193948Z-8bc886ed-output-observed-qwen-followup-capture/bridge-raw-output.txt`
- `integration/runtime/artifacts/p6d-20261006T193948Z-8bc886ed-output-observed-qwen-followup-capture/fake-complete-response.sse`
- `integration/runtime/artifacts/p6d-20261006T193948Z-8bc886ed-output-observed-qwen-followup-capture/fake-incomplete-response.sse`
- `integration/runtime/artifacts/p6d-20261006T193948Z-8bc886ed-output-observed-qwen-followup-capture/postgres-raw-output.txt`
- `integration/runtime/artifacts/p6d-20261006T193948Z-8bc886ed-output-observed-qwen-followup-capture/provider-choice-0.txt`
- `integration/runtime/artifacts/p6d-20261006T193948Z-8bc886ed-output-observed-qwen-followup-capture/provider-response.sse`
- `integration/runtime/artifacts/p6d-20261006T193948Z-8bc886ed-output-observed-qwen-followup-capture/sdk-assistant.txt`
- `integration/runtime/artifacts/p6d-20261006T193948Z-8bc886ed-output-observed-qwen-followup-capture/sdk-result.txt`
- `integration/runtime/artifacts/p6d-20261006T193948Z-8bc886ed-output-observed-qwen-followup.json`

## 다음 작업

1. 후보 변경을 검토하고 기준 커밋을 만든다. 이번 정리에서는 stage·commit하지 않았다.
2. 기존 CLI·worker의 일반 로컬 실행 설정과 모델 profile admission을 연결한다.
3. 실제 Qwen으로 Evidence·Critic·synthesis·Position·projection 경로를 제한된 업무 사례에서 확인한다.
4. HTTP API·운영자 UNKNOWN 복구·평가 runner의 v0.1 포함 여부를 원 명세와 대조한다. 포함된다면 구현해야 한다.

추가 테스트를 늘리기 전에 일반 실행 연결과 미구현 인터페이스의 출시 범위를 확정한다.
