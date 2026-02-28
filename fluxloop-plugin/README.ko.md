[English](README.md) | **한국어**

# FluxLoop Claude Code Plugin

AI 에이전트 테스트 및 평가를 자동화하는 Claude Code 플러그인입니다.

## 🚀 설치

### Step 1: Marketplace 추가

```
/plugin marketplace add Fluxloop-AI/fluxloop-claude-plugin
```

### Step 2: 플러그인 설치

```
/plugin install fluxloop@fluxloop
```

끝! 이제 Claude에게 자연어로 말하면 됩니다.

---

## ⭐ Skills — 자연어로 에이전트를 테스트하세요

FluxLoop는 6개의 전문 스킬로 구성되어 있습니다. 자연어로 말하면 적절한 스킬이 자동 활성화됩니다.

### 🗣️ 사용법 (자연어)

| 의도 | 예시 | 활성화되는 스킬 |
|------|------|----------------|
| 처음 세팅 | "fluxloop 세팅해줘" | setup |
| 에이전트 파악/업데이트 | "에이전트 파악해줘" | context |
| 시나리오 생성 | "시나리오 만들어줘" | scenario |
| 테스트 실행 | "테스트 돌려줘" | test |
| 평가/개선 | "평가해줘" | evaluate |
| 프롬프트 비교 | "프롬프트 비교해줘" | prompt-compare |

### 스킬 워크플로우

**최초 설정 (프로젝트당 1회):**
```
setup → context → scenario → test → evaluate
```

**일상 루프 (대부분의 시간):**
```
test → evaluate → (코드 수정) → test → evaluate → ...
```

> setup과 context는 이미 완료된 경우 자동으로 생략됩니다.
> 바로 "테스트 돌려줘"라고 말하면 FluxLoop가 현재 상태를 감지하고 알아서 처리합니다.

**명령어를 외울 필요 없습니다. 수동 작업도 없습니다. 그냥 말하세요.**

---

## 📋 요구사항

- **FluxLoop 계정**: [alpha.app.fluxloop.ai](https://alpha.app.fluxloop.ai)
- **Node.js 18+**: FluxLoop CLI 실행에 필요

> 💡 **setup 스킬이 FluxLoop CLI를 자동으로 설치합니다!** "fluxloop 세팅해줘"라고 말하면 모든 설정이 자동으로 완료됩니다.

---

## 🔧 대화 예시

```
You: "fluxloop 세팅해줘"
Claude: [setup skill] CLI 설치, 인증, 프로젝트 설정을 진행합니다...
      ✅ Setup complete → "에이전트 파악해줘"로 다음 단계를 진행하세요.

You: "에이전트 파악해줘"
Claude: [context skill] 코드베이스를 스캔하여 에이전트 프로필을 생성합니다...
      ✅ Profile saved → "시나리오 만들어줘"로 다음 단계를 진행하세요.

You: "테스트 돌려줘"
Claude: [test skill] 시나리오와 데이터를 확인 후 테스트를 실행합니다...
      ✅ Test completed! 47/50 passed (94%)
      → "평가해줘"로 결과를 분석할 수 있습니다.
```

---

## 🧪 평가

FluxLoop는 AI 인사이트 기반의 서버 사이드 평가를 제공합니다.

**"평가해줘"** 라고 말하면 **evaluate** 스킬이 모든 것을 자동으로 처리합니다.

CLI를 직접 사용하려면 [문서](https://docs.fluxloop.ai)를 참고하세요.

---

## 🪝 Hooks (선택)

파일 편집 후 스모크 테스트를 자동 실행합니다 (FluxLoop가 설정된 경우에만):

```json
{
  "hooks": [
    {
      "type": "PostToolUse",
      "matcher": "Write|Edit",
      "command": "if [ -f .fluxloop/context.json ] && command -v fluxloop >/dev/null 2>&1; then fluxloop test --smoke --quiet; fi"
    }
  ]
}
```

> FluxLoop CLI가 설치되어 있고 프로젝트가 초기화된 경우에만 hook이 실행됩니다.

---

## 📁 프로젝트 구조

```
your-project/
├── .fluxloop/
│   ├── project.json          # 프로젝트 연결 정보
│   ├── context.json          # 현재 시나리오 포인터
│   ├── .env                  # API 키
│   ├── scenarios/
│   │   └── my-test/
│   │       ├── agents/       # 에이전트 래퍼
│   │       ├── configs/      # 설정 파일
│   │       ├── contracts/    # 시나리오 계약 (YAML)
│   │       ├── inputs/       # 테스트 입력 데이터
│   │       └── experiments/  # 테스트 결과
│   └── test-memory/          # 스킬 간 공유 컨텍스트 (자동 생성)
│       ├── agent-profile.md  # 에이전트 프로필 및 메타데이터
│       ├── test-strategy.md  # 테스트 목표 및 기준
│       ├── prompt-versions.md # 프롬프트 버전 이력
│       ├── results-log.md    # 테스트 결과 로그
│       └── learnings.md      # 인사이트 및 개선사항
└── fluxloop.yaml             # 프로젝트 설정
```

## 🔗 링크

- **FluxLoop Web**: <a href="https://alpha.app.fluxloop.ai" target="_blank">alpha.app.fluxloop.ai</a>
- **Documentation**: <a href="https://docs.fluxloop.ai" target="_blank">docs.fluxloop.ai</a>

## 📄 라이선스

MIT License
