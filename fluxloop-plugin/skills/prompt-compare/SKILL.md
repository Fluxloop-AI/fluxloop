---
name: fluxloop-prompt-compare
description: |
  Use for prompt version comparison and stability testing.
  Keywords: compare, comparison, prompt version, stability, A/B test, diff

  Auto-activates on requests like:
  - "compare prompts", "프롬프트 비교해줘"
  - "stability test", "안정성 테스트"
  - "v3 vs v4", "버전 비교"
  - "run same input multiple times", "같은 인풋으로 여러번 돌려줘"
---

# FluxLoop Prompt Compare Skill

Compare prompt versions by running the same bundle multiple times and analyzing output differences.

## Core Principle

**Same Bundle × N Repeats × Version Diff** — freeze inputs via bundle, automate repeated runs, compare outputs.

## Prerequisite

Requires a FluxLoop scenario to exist (agent-test Phase 1~2 completed).
If no scenario exists, guide: `"먼저 시나리오 셋업이 필요합니다. 'test my agent'로 시작하세요."`

> **Staging:** 사용자가 "staging"을 언급하면 setup 단계에서 `--staging` 플래그를 추가합니다.
> `fluxloop auth login --staging`, `fluxloop projects select <id> --staging`

---

## Phase 0: Context Check

```bash
fluxloop context show
ls .fluxloop/scenarios
```

| State | Action |
|-------|--------|
| No scenario | → Guide to agent-test skill |
| Scenario exists | → Phase 1 |

---

## Phase 1: Bundle Selection

Inputs come from Web — generated or selected from existing bundles. Follow the same decision tree as agent-test Phase 3.

```bash
fluxloop bundles list --scenario-id <scenario_id>
```

```
bundles list
  │
  ├─ Bundles exist → Show list (version, input count, date), ask "Which bundle?"
  │   └─ User selects → Use it
  │
  └─ No bundle → inputs list --scenario-id <id>
      │
      ├─ Input sets exist → Show list, ask "Which one?"
      │   └─ User selects → Publish as bundle
      │       fluxloop bundles publish --scenario-id <id> --input-set-id <id>
      │
      └─ No inputs → Generate small set for comparison
          fluxloop personas suggest --scenario-id <id>
          fluxloop inputs synthesize --scenario-id <id> --total-count 2
          fluxloop bundles publish --scenario-id <id> --input-set-id <id>
```

> **Tip:** 비교 테스트에는 인풋 1-3개면 충분합니다. 새로 생성할 때 `--total-count 2`로 소규모 번들을 만드세요.

Key info to display: **version/name, tag/description, input count, created date**

After bundle selected/created:

```bash
fluxloop sync pull --bundle-version-id <bundle_version_id>
```

> This bundle stays fixed throughout all comparison runs. Record the `bundle_version_id` for reuse.

---

## Phase 2: Comparison Setup

Ask the user:

```
1. 반복 횟수? (기본: 5)
2. 멀티턴? (기본: 싱글턴) → yes일 경우 최대 턴 수도 확인
3. 현재 프롬프트 버전 라벨? (예: "v3", "기존 버전")
```

Set iterations in `configs/simulation.yaml`:

```yaml
iterations: 5  # user-specified count
```

> Read existing simulation.yaml first. Only modify `iterations`, preserve all other fields.

---

## Phase 3: Baseline Run (Version A)

### 3-1. Capture git diff (code snapshot)

```bash
git diff HEAD
```

Record the diff output — this captures the current code state before the run.

### 3-2. Run

```bash
# Single-turn (default)
fluxloop test --scenario <name>

# Multi-turn
! fluxloop test --scenario <name> --multi-turn --max-turns <N>
```

> ⚠️ Multi-turn requires `!` prefix.

After completion:
1. Note the experiment directory: `.fluxloop/scenarios/<name>/experiments/exp_<timestamp>/`
2. Record as `experiment_A` with the user's version label + git diff
3. Output: `✅ Baseline → exp_<timestamp> (라벨: "v3", N runs)`

---

## Phase 4: Prompt Modification

```
프롬프트를 수정해주세요.
수정이 완료되면 알려주세요. 새 버전 라벨도 알려주세요 (예: "v4").
```

Wait for user confirmation. Do NOT modify any code yourself.

---

## Phase 5: Variant Run (Version B)

### 5-1. Capture git diff (what changed)

```bash
git diff HEAD
```

This shows exactly what the user changed between v3 and v4. Record for the comparison report.

### 5-2. Run

Same bundle, same inputs — only the prompt changed.

```bash
# Single-turn
fluxloop test --scenario <name>

# Multi-turn (same settings as baseline)
! fluxloop test --scenario <name> --multi-turn --max-turns <N>
```

> No need to `sync pull` again. The bundle is already pulled locally.

After completion:
1. Note the experiment directory as `experiment_B`
2. Output: `✅ Variant → exp_<timestamp> (라벨: "v4", N runs)`

---

## Phase 6: Comparison Analysis

### 6-1. Load Results

Read both experiment trace files:

```
.fluxloop/scenarios/<name>/experiments/<exp_A>/trace_summary.jsonl
.fluxloop/scenarios/<name>/experiments/<exp_B>/trace_summary.jsonl
```

Each line in trace_summary.jsonl:
```json
{
  "trace_id": "uuid",
  "iteration": 0,
  "persona": "persona_name",
  "input": "user input text",
  "output": "agent response",
  "duration_ms": 1234.5,
  "success": true,
  "token_usage": {"input_tokens": 10, "output_tokens": 20}
}
```

### 6-2. Analyze & Report

Generate a comparison report with these sections:

#### 1) Prompt Changes (git diff summary)

```markdown
## 프롬프트 변경 내용 (v3 → v4)
- [변경된 파일과 핵심 수정 내용 요약]
```

#### 2) Per-Input Analysis

Group traces by `input` field, then compare across versions:

```markdown
## 인풋: "데이터 분석할래"

### v3 (5회)
| # | 출력 요약 | 토큰 | 시간(ms) |
|---|----------|------|----------|
| 1 | [핵심 1줄 요약] | 150 | 1200 |
| 2 | ... | ... | ... |

### v4 (5회)
| # | 출력 요약 | 토큰 | 시간(ms) |
|---|----------|------|----------|
| 1 | [핵심 1줄 요약] | 130 | 1100 |
| 2 | ... | ... | ... |

### 비교
- **버전 내 일관성**: v3 높음 / v4 보통
- **버전 간 차이**: 뚜렷함 / 미미함
- **핵심 변화**: [구체적으로 무엇이 달라졌는지]
```

#### 3) Overall Summary

```markdown
## 종합 비교

| 지표 | v3 | v4 |
|------|----|----|
| 평균 토큰 수 | 150 | 130 |
| 평균 응답 시간(ms) | 1200 | 1100 |
| 성공률 | 100% | 100% |
| 버전 내 일관성 | 높음 | 보통 |

### 결론
[프롬프트 변경 내용 → 결과 변화를 연결해서 효과 요약]
```

---

## Phase 7: Next Actions

```
다음 중 선택하세요:
1. 추가 비교 — 프롬프트를 다시 수정하고 비교 (→ Phase 4)
2. Web 상세 보기 — experiment URL 제공
3. 서버 평가 — fluxloop evaluate로 상세 분석
4. 완료
```

If "추가 비교": loop back to Phase 4 (same bundle reused).
If "서버 평가":
```bash
fluxloop evaluate --experiment-id <exp_B_id> --wait
```

---

## Quick Reference

| Phase | Action | Command |
|-------|--------|---------|
| 0 | Context check | `fluxloop context show` |
| 1 | Bundle selection | `fluxloop bundles list`, `sync pull --bundle-version-id <id>` |
| 2 | Set iterations + multi-turn | Edit `simulation.yaml`, decide multi-turn |
| 3 | Baseline run | `git diff HEAD` + `fluxloop test --scenario <name>` |
| 4 | User modifies prompt | (wait) |
| 5 | Variant run | `git diff HEAD` + `fluxloop test --scenario <name>` |
| 6 | Compare | Read trace_summary.jsonl × 2 + git diffs, generate report |
| 7 | Next action | Loop / evaluate / done |

---

## Key Rules

1. **Inputs come from Web** — generate via `inputs synthesize` or select from existing, never write base_inputs manually
2. **Small bundles for comparison** — recommend `--total-count 2` when generating new inputs for comparison
3. **Bundle = frozen inputs** — pull once, reuse across all comparison runs
4. **Never modify the user's prompt/agent code** — only the user does that
5. **Capture git diff before each run** — links code changes to result changes
6. **Only change `iterations` in simulation.yaml** — preserve all other config fields
7. **Always read trace_summary.jsonl** — it has per-run details needed for comparison
8. **Label experiments clearly** — use user-provided version labels throughout
9. **Multi-turn uses `!` prefix** — same as agent-test skill
