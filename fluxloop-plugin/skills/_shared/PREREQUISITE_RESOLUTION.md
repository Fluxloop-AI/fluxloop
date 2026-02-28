# Prerequisite Resolution Protocol

스킬 실행 중 prerequisite가 누락된 경우, 사용자에게 "먼저 X해주세요"라고 안내하고 중단하지 않는다.
대신 아래 절차를 따라 선행 스킬을 인라인 실행한다.

## 의존성 체인

```
setup → context → scenario → test → evaluate
```

## Resolution 절차

1. **CLI 설치 확인**: `fluxloop --version` 실행 → `command not found` 시 setup 스킬의 설치 단계부터 시작
2. **누락 범위 파악**: `fluxloop context show` + 로컬 파일 존재 여부로 어디부터 필요한지 식별
3. **사용자 확인**: 필요한 선행 단계를 나열하고 한 번에 확인
   - 1개 누락: "{스킬명}이 아직 완료되지 않았습니다. 먼저 진행할까요?"
   - 다단계 누락: "{스킬A} → {스킬B} 순서로 진행이 필요합니다. 순서대로 진행할까요?"
4. **승인 시**: 필요한 스킬을 순서대로 인라인 실행
   - 각 스킬의 SKILL.md 절차를 따름
   - 각 완료 시: "✅ {스킬명} 완료 — {해당 스킬이 한 일을 1문장으로 요약}. 이어서 {다음}을 진행합니다."
     - 예: "✅ Setup 완료 — CLI 설치 및 프로젝트 연결이 완료되었습니다. 이어서 context를 진행합니다."
     - 예: "✅ Context 완료 — 에이전트 코드를 분석하여 프로필을 생성했습니다. 이어서 scenario를 진행합니다."
   - 모든 선행 완료 후: 원래 요청한 스킬의 Step 1로 자동 복귀
5. **거부 시**: 현재 스킬을 중단하고, 어떤 선행 스킬이 필요한지만 안내

## 인라인 실행 규칙

- 선행 스킬의 **핵심 절차만** 수행 — 마무리 안내("다음에는 X를 해보세요")는 생략
- 선행 스킬 실행 중 에러 발생 시 → 해당 에러를 사용자에게 보고하고 **전체 중단** (원래 스킬까지 진행하지 않음)
- setup 스킬은 사용자 인터랙션(로그인, 프로젝트 선택)이 필요하므로 해당 단계는 그대로 유지
- 한 번의 확인으로 필요한 모든 선행 스킬을 순서대로 실행 (매 스킬마다 재확인하지 않음)

## 상태 판별 기준

| 체크 대상 | 확인 방법 | 누락 시 필요한 스킬 |
|-----------|-----------|---------------------|
| CLI 설치 | `fluxloop --version` → command not found | setup (설치 단계) |
| 프로젝트 설정 | `fluxloop context show` → project 없음 | setup (인증/프로젝트 단계) |
| 에이전트 프로필 | `.fluxloop/test-memory/agent-profile.md` 부재 | context |
| 시나리오 | `fluxloop context show` → scenario 없음 | scenario |
| 테스트 결과 | `.fluxloop/test-memory/results-log.md` 부재 또는 엔트리 0건 | test |

## Skip 메시지 (이미 완료된 경우)

Prerequisite가 이미 충족된 상태에서 인라인 실행이 트리거될 때, 해당 스킬을 생략하며 한 줄로 이유를 알린다.

| 상태 | 메시지 |
|------|--------|
| setup (설치) 완료 | "✅ fluxloop-cli 설치됨 (vX.X.X) — 설치 단계 생략" |
| setup (프로젝트) 완료 | "✅ 프로젝트 연결됨 (proj_xxx) — setup 생략" |
| context 완료 (최신) | "✅ 에이전트 프로필 최신 ({N}일 전 스캔) — context 생략" |
| context 완료 (stale) | "⚠️ 프로필이 오래됨 (코드 변경 감지) — 업데이트 권장 (선택사항)" |
| scenario 완료 | "✅ 활성 시나리오 있음 ({scenario_name}) — scenario 생략" |
| test 완료 | "✅ 최근 테스트 결과 있음 (exp_xxx) — 바로 평가 진행" |
