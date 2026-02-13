---
name: fluxloop-agent-test
description: |
  Use for AI agent testing, simulation, and test data generation requests.
  Keywords: test, simulation, input synthesis, scenario, agent evaluation

  Auto-activates on requests like:
  - "test this agent", "create test"
  - "run simulation"
  - "generate test data"
  - "evaluate agent"
---

# FluxLoop Agent Test Skill

Manages the complete test cycle for AI agents.

## Core Principle

**Context-First:** Check context → summarize → confirm → execute

```
1. Check context (fluxloop context show)
2. Summarize current state
3. Present options
4. Execute after user confirmation
5. Save results to context
```

### Execution Mode (Ask once at start)

| Mode | Description |
|------|-------------|
| 🚀 **Auto** | Run all phases without stopping, only pause on errors |
| ✅ **Interactive** (default) | Pause after each major step for confirmation |

Prompt: `"Run automatically or step-by-step? (auto/interactive, default: interactive)"`

---

## Terminology

| Term | Description |
|------|-------------|
| **Web Project** | Remote project on FluxLoop cloud (`project_id`) |
| **Web Scenario** | Remote scenario on FluxLoop cloud (`scenario_id`) |
| **Local Scenario** | Local folder at `.fluxloop/scenarios/<name>/` |
| **Input Set** | Generated test inputs (`input_set_id`) |
| **Bundle** | Published snapshot of inputs + personas (`bundle_version_id`) |

### Workspace Structure
```
project_root/
  .fluxloop/
    project.json          # Web Project connection
    context.json          # Current scenario pointer
    .env                  # API key (shared by all scenarios)
    scenarios/
      scenario-a/
        .env              # Scenario-specific env (OPENAI_API_KEY, etc.)
        agents/           # Agent wrapper files
          wrapper.py
        configs/          # simulation.yaml, evaluation.yaml
        contracts/        # Scenario contracts (YAML)
        inputs/           # Generated test inputs
        experiments/      # Test results
      scenario-b/
        ...
```

---

## Post-Action Summary

After each action, output: `✅ [Action] → [summary] 🔗 [url]`

| Action | URL Pattern |
|--------|-------------|
| Data | (no URL) |
| Project | `https://alpha.app.fluxloop.ai/simulate/scenarios?project={project_id}` |
| Scenario | `https://alpha.app.fluxloop.ai/simulate/scenarios/{scenario_id}?project={project_id}` |
| Contracts | same as Scenario URL |
| Input Set | `https://alpha.app.fluxloop.ai/simulate/scenarios/{scenario_id}/inputs/{input_set_id}?project={project_id}` |
| Bundle | `https://alpha.app.fluxloop.ai/simulate/scenarios/{scenario_id}/bundles/{bundle_version_id}?project={project_id}` |
| Experiment | `https://alpha.app.fluxloop.ai/evaluate/experiments/{experiment_id}?project={project_id}` |

Examples:
- `✅ Data → 3 files uploaded to project library`
- `✅ Contracts → 3개 생성됨 🔗 [scenario URL]`
- `✅ Scenario → "Order Bot" (scn_abc) 🔗 .../simulate/scenarios/scn_abc?project=proj_123`

---

## Phase 0: Context Check (Always First!)

```bash
fluxloop context show    # Check current state
fluxloop auth status     # Check login status
ls .fluxloop/scenarios   # Check local scenario folders (from workspace root)
```

### State-Based Actions

| Context State | Next Action |
|---------------|-------------|
| None (no context.json) | → Phase 1: Setup |
| Project only | → Phase 2: Create Scenario |
| Scenario exists, no data | → Phase 3: Generate Data |
| Bundle exists | → Phase 4: Run Test |

---

## Phase 1: Setup (One-time)

```bash
# 0. Check if installed
fluxloop --version

# If not installed, add to workspace environment (match your project's setup):
# - uv:     uv add fluxloop-cli     → run with: uv run fluxloop ...
# - pip:    pip install fluxloop-cli → run with: fluxloop ... (venv activated)
# - poetry: poetry add fluxloop-cli → run with: poetry run fluxloop ...
# Priority: Install in the SAME environment where your agent runs (important for simulation)

# 1. Login (for agents: prints code then polls)
fluxloop auth login --no-wait && fluxloop auth login --resume

# Staging: 사용자가 "staging"을 언급하면 --staging 플래그 추가
# fluxloop auth login --staging --no-wait && fluxloop auth login --resume
# fluxloop projects select <id> --staging
# fluxloop projects create --name "my-agent" --staging

# 2. Select or create project
fluxloop projects list
fluxloop projects select <project_id>
# OR
fluxloop projects create --name "my-agent"
fluxloop intent refine --intent "..."
```

### Naming Rules (for create commands)

| Field | Language | Format |
|-------|----------|--------|
| Folder name | **English only** | kebab-case (`order-bot`) |
| Project/Scenario name | Any | Display text ("주문 봇", "Order Bot") |

**When creating project/scenario:** Suggest 3 candidates based on codebase:
- Analyze: `package.json`, `pyproject.toml`, main agent file, README, directory name
- Folder: keywords → kebab-case → suffix (`-agent`, `-bot`)
- Display: offer Korean + English options

```
📁 Folder: 1) order-bot  2) food-agent  3) cs-service
📝 Name: 1) Order Bot  2) 주문 봇  3) Food Agent
Select number or type custom:
```

> **Important**: Install fluxloop-cli in your workspace's environment so simulations run with the same dependencies as your agent.
> For detailed setup instructions, run `/fluxloop:setup`

---

## Phase 2: Create Scenario (Once per scenario)

> ⚠️ **Important:** Run from workspace root (where `.fluxloop/` should be created).
> Phase 1 (`projects select`) must be done first to establish the workspace.

```bash
# 0. Ensure you're in workspace root (not home directory!)
pwd  # Should be your project directory, NOT ~

# 1. Initialize local folder (creates .fluxloop/scenarios/<name>/)
fluxloop init scenario order-bot  # ← Use Naming Rules (English kebab-case)
```

### Step 2: Context Collection

Scan the workspace to understand the project before creating a scenario.

1. **Auto-scan** codebase: README, main agent files, package.json/pyproject.toml, API specs, test files
2. Summarize: project type, key features, dependencies, relevant docs
3. **Interactive only:** Ask `"참고할 문서가 있나요? (경로 입력 / 스킵)"`
4. Upload key files to project library:

```bash
fluxloop data push README.md
fluxloop data push docs/api-spec.md --bind  # --bind attaches to current scenario
```

### Step 3: Scenario Recommendation → Create

Suggest 3 scenarios based on collected context:

| # | Type | Description |
|---|------|-------------|
| 1 | **Happy Path** | Core feature verification |
| 2 | **Edge Cases** | Exception/boundary handling |
| 3 | **Advanced** | Multi-turn or domain-specific |

- **Interactive:** Present 3 options + "직접 입력", user selects
- **Auto:** Select #1 automatically

```bash
fluxloop scenarios create --name "주문 정확성 테스트" --goal "..."
fluxloop scenarios refine --scenario-id <id>
```

### Step 4: Contracts

After `scenarios refine`, server auto-generates contracts. Pull them locally:

```bash
fluxloop sync pull  # Saves contracts to .fluxloop/scenarios/<name>/contracts/
```

Output: `✅ Contracts → 3개 생성됨 🔗 [scenario URL]`
Guide user: `📋 웹에서 contracts를 확인/수정할 수 있습니다`

### Step 5–6: API Key & Wrapper

```bash
# 5. Create API key (saves to .fluxloop/.env, shared by all scenarios)
fluxloop apikeys create

# 6. Set up agent wrapper (if complex agent - see "Agent Wrapper Setup" section)
#    Create: .fluxloop/scenarios/<name>/agents/wrapper.py
#    Update: configs/simulation.yaml → runner.target: "agents.wrapper:run"
```

### Phase 2 Interactive Checkpoints

| Step | Interactive | Auto |
|------|------------|------|
| Context: 참고 문서 | Ask | Skip |
| Scenario: 3개 중 선택 | Ask (required) | Auto-select #1 |
| Contracts: 확인 | URL only | URL only |

> Max 2 required user responses in Interactive mode.
> **Common mistake:** Running `init scenario` from home directory creates in `~/.fluxloop/` instead of workspace.

---

## Phase 3: Generate Data (Decision Tree)

**Always check existing data with list commands, then ask user:**

```
bundles list --scenario-id <id>
  │
  ├─ Multiple bundles found → Show list with details, ask "Which bundle?"
  │   │
  │   └─ User selects → Go to Phase 4 (2 commands)
  │
  ├─ One bundle found → "Use existing or create new?"
  │   │
  │   ├─ Use existing → Go to Phase 4 (2 commands)
  │   │
  │   └─ Create new → Check inputs list
  │
  └─ No bundle → inputs list --scenario-id <id>
                   │
                   ├─ Multiple input sets → Show list with details, ask "Which one?"
                   │   │
                   │   └─ User selects → Publish bundle only (3 commands)
                   │
                   ├─ One input set found → "Use existing or create new?"
                   │   │
                   │   ├─ Use existing → Publish bundle only (3 commands)
                   │   │
                   │   └─ Create new → Full generation (5 commands)
                   │
                   └─ No input set → Full generation
```

### When Multiple Resources Exist

Show identifying information to help user choose:

```
Agent: Found 3 existing bundles:
       1. v3 (stress-test, 20 inputs, 1 day ago)
       2. v2 (happy-path, 5 inputs, 3 days ago)
       3. v1 (edge-cases, 10 inputs, 7 days ago)
       
       Which bundle to use? Or create new?
```

Key info to display: **version/name, tag/description, count, created date**

### Commands by Path

**Use existing bundle (2 commands):**
```bash
fluxloop sync pull --bundle-version-id <id>  # Auto-saves to current scenario
fluxloop test --scenario <name>
```

**Use existing input set (3 commands):**
```bash
fluxloop bundles publish --scenario-id <id> --input-set-id <id>
fluxloop sync pull --bundle-version-id <id>  # Auto-saves to current scenario
fluxloop test --scenario <name>
```

**Full generation (5~7 commands):**
```bash
fluxloop personas suggest --scenario-id <id>
fluxloop inputs synthesize --scenario-id <id>  # Use --timeout 300 for large sets, --total-count 2 for quick small test

# (Interactive only) QC loop — Auto mode skips this entirely
fluxloop inputs qc --scenario-id <id> --input-set-id <id>
# → Show results (format, duplicates, diversity, quality score)
# → Ask "개선할까요?" → if yes:
fluxloop inputs refine --scenario-id <id> --input-set-id <id>  # Returns new input_set_id

fluxloop bundles publish --scenario-id <id> --input-set-id <id>
fluxloop sync pull --bundle-version-id <id>    # Auto-saves to current scenario
fluxloop test --scenario <name>
```

---

## Phase 4: Run Test

**Pre-check:** Ensure wrapper is configured (see "Agent Wrapper Setup" section)

```bash
# Always run sync pull and test separately
fluxloop sync pull --bundle-version-id <id>   # Auto-uses current scenario
fluxloop test --scenario <name>
```

> ⚠️ Do NOT use `test --pull`. Always run `sync pull` + `test` separately.

### Multi-Turn Testing

테스트 전 확인: `"멀티턴? (yes/no), 최대 턴? (default:8), LLM? (openai/anthropic)"`

#### API Key 설정 (유저 직접 수행)

멀티턴은 LLM API Key 필요. 아래 파일 경로를 출력하면 유저가 클릭해서 편집 가능:
```
📁 API Key 설정이 필요합니다. 아래 파일을 열어 추가해주세요:
.fluxloop/scenarios/<scenario>/.env

추가할 내용:
- OpenAI: OPENAI_API_KEY=sk-xxx
- Anthropic: ANTHROPIC_API_KEY=sk-ant-xxx
```

#### 실행

```
# OpenAI (기본)  — 반드시 ! prefix 를 붙여 실행
! fluxloop test --scenario <name> --multi-turn --max-turns 5

# Anthropic  — 반드시 ! prefix 를 붙여 실행
! fluxloop test --scenario <name> --multi-turn --supervisor-provider anthropic
```

> ⚠️ 멀티턴 테스트는 반드시 명령어 앞에 느낌표(!) prefix를 붙여야 합니다

### View Results
```bash
fluxloop test results --scenario <name>    # Formatted output
fluxloop test results --scenario <name> --raw  # Raw markdown
```

---

## Phase 5: Evaluate (Server-side Insights)

Use server-side evaluation to generate insights and recommendations for an experiment.

```bash
fluxloop evaluate --experiment-id <id>
fluxloop evaluate --experiment-id <id> --wait
fluxloop evaluate --experiment-id <id> --wait --timeout 900 --poll-interval 5
```

### Evaluation Notes
- `--wait` polls until status is `completed`, `partial`, `failed`, or `cancelled`.
- When a job finishes as `completed` or `partial` with at least one run completed, insights/recommendations are generated and the CLI prints the latest headlines.
- If the job stays `queued` for >30s without `locked_at`, the CLI warns that workers may be down or backlog is high.

### Web Handoff

After evaluation completes, output:

```
✅ Evaluation → N insights 🔗 [experiment URL]
📋 웹에서 상세 분석을 확인하세요:
  - Decision: gates, budgets, baseline 비교
  - Insights: 카테고리별 발견사항 (severity 표시)
  - Recommendations: 개선 제안 (priority 표시)
  - Baseline: 현재 결과를 baseline으로 설정 가능
💡 에이전트 개선: 'improve my agent' → Phase 6
```

---

## Phase 6: Improve & Re-test (Iteration Loop)

Triggered by: Phase 5 completion, `"improve my agent"`, `"에이전트 개선해줘"`, `"re-test"`

```bash
# [6-1] Sync latest state & analyze results
fluxloop sync pull --bundle-version-id <id>
fluxloop test results --scenario <name>
# → Claude Code analyzes: warning turns, failure patterns, contract violations
# → Identify related code locations

# [6-2] Fix agent code
# → Suggest fixes based on analysis → ALWAYS require user confirmation (even in Auto mode)
# → After edit, hook runs: fluxloop test --smoke --quiet

# [6-3] Re-test with same bundle
fluxloop sync pull --bundle-version-id <id>   # Same bundle
fluxloop test --scenario <name>

# [6-4] Re-evaluate → Web handoff
fluxloop evaluate --experiment-id <new_id> --wait
# → Output: "✅ Re-evaluation → N insights 🔗 [URL]"
# → Guide: "📋 웹에서 이전 baseline과 비교하세요"
```

> ⚠️ **Code changes always require user confirmation**, even in Auto mode.

| Step | Interactive | Auto |
|------|------------|------|
| [6-1] Result analysis | Show + confirm | Show only |
| [6-2] Code fix | Suggest → confirm (required) | Suggest → confirm (required) |
| [6-3] Re-test | Ask "재테스트?" | Auto-proceed |
| [6-4] Re-evaluate | Ask "재평가?" | Auto-proceed |

Repeat loop: unsatisfied → back to [6-1]

---

## Agent Wrapper Setup

To run tests, FluxLoop needs to invoke your agent via `runner.target` in `configs/simulation.yaml`.

### When is Wrapper Needed?

| Agent Type | Wrapper? |
|------------|----------|
| Simple function `def run(input: str) -> str` | ❌ Direct call |
| Class/stateful agent | ✅ Wrap initialization |
| External dependencies (DB, broker, API) | ✅ Wrap dependency injection |

### Setup Steps

1. Create: `.fluxloop/scenarios/<name>/agents/wrapper.py`
2. Update: `configs/simulation.yaml` → `runner.target: "agents.wrapper:run"`
3. Debug: `python -c "from agents.wrapper import run; print(run('test'))"`

> See **Appendix A1** for full wrapper template

---

## Quick Reference

| Command | Description |
|---------|-------------|
| `fluxloop context show` | Check current state |
| `fluxloop auth login` | Login (add `--staging` for staging) |
| `fluxloop projects select <id>` | Select project (add `--staging` for staging) |
| `fluxloop init scenario <name>` | Create local scenario folder |
| `fluxloop scenarios create --name X --goal "..."` | Create web scenario |
| `fluxloop scenarios select <id>` | Select scenario (auto-detects local folder) |
| `fluxloop scenarios select <id> --local-path <folder>` | Select with explicit local folder |
| `fluxloop apikeys create` | Create API key (saves to `.fluxloop/.env`) |
| `fluxloop bundles list --scenario-id <id>` | List existing bundles |
| `fluxloop personas suggest --scenario-id <id>` | Generate personas |
| `fluxloop inputs synthesize --scenario-id <id>` | Generate inputs (use `--timeout 300` for large) |
| `fluxloop bundles publish --scenario-id <id> --input-set-id <id>` | Publish bundle |
| `fluxloop sync pull --bundle-version-id <id>` | Pull bundle (auto-uses current scenario) |
| `fluxloop test --scenario <name>` | Run test |
| `fluxloop test --scenario <name> --multi-turn` | Run multi-turn test |
| `fluxloop test --scenario <name> --multi-turn --max-turns 5` | Multi-turn test with max 5 turns |
| `fluxloop test results --scenario <name>` | View latest test results |
| `fluxloop data push <file> --bind` | Upload file to project library (--bind attaches to scenario) |
| `fluxloop inputs qc --scenario-id <id> --input-set-id <id>` | Quality check on inputs (Interactive only) |
| `fluxloop inputs refine --scenario-id <id> --input-set-id <id>` | AI-based input improvement |
| `fluxloop evaluate --experiment-id <id> --wait` | Trigger evaluation and show insight/reco headlines |

---

## Error Handling

| Error | Solution |
|-------|----------|
| `Login required` | `fluxloop auth login` |
| `No project selected` | `fluxloop projects select <id>` |
| `Sync API key not set` | `fluxloop apikeys create` |
| `Inputs file not found` | `fluxloop sync pull --bundle-version-id <id>` |
| `No personas found` | `fluxloop personas suggest --scenario-id <id>` first |
| `Synthesis timed out` | Use `--timeout 300` or reduce `--total-count` |
| Scenario created in `~/.fluxloop/` | Run from workspace root, not home. Do `projects select` first. |
| Local path mismatch in context | `fluxloop scenarios select <id> --local-path <folder>` |
| `ModuleNotFoundError` in test | Check `runner.target` in simulation.yaml, ensure wrapper is in Python path |
| `TypeError: run() missing argument` | Wrapper must accept `(input_text: str, metadata: dict = None)` |
| Agent returns None | Ensure wrapper returns string, not None |

---

## Key Takeaways

1. **Ask execution mode at start** (Auto or Interactive)
2. **Always check context first** (`fluxloop context show`)
3. **Phase 2: Recommend scenarios** — scan codebase, suggest 3, avoid blank-page problem
4. **Phase 2: Auto-generate contracts** — `sync pull` after `scenarios refine`
5. **Check existing data with list** (`bundles list`, `inputs list`)
6. **Use Naming Rules** (English kebab-case for folders, any language for display names)
7. **Output summary after each action** (`✅ Action → summary 🔗 url`)
8. **Run sync pull + test separately** (Do NOT use `--pull`)
9. **Use explicit IDs** (`--bundle-version-id`, `--scenario-id`)
10. **Phase 5 → Web handoff** — guide user to web for detailed analysis
11. **Phase 6: Iteration loop** — analyze → fix (always confirm) → re-test → re-evaluate
12. **Complex agents need wrapper** (See "Agent Wrapper Setup" + Appendix A1)
13. **Multi-turn: ask settings + use 느낌표(!) prefix** (! fluxloop test --multi-turn)

---

## Appendix

### A1. Wrapper Template (Full)

```python
# .fluxloop/scenarios/<name>/agents/wrapper.py
import uuid
from my_agent import AgentService

_agent = None

def run(input_text: str, metadata: dict = None) -> str:
    """FluxLoop test entry point. Must return string."""
    global _agent
    if _agent is None:
        _agent = AgentService()  # Initialize once
    
    conversation_id = str(uuid.uuid4())
    response = _agent.process(conversation_id, input_text)
    return str(response)
```

```yaml
# configs/simulation.yaml
runner:
  target: "agents.wrapper:run"
```

**Async version:**
```python
def run(input_text: str, metadata: dict = None) -> str:
    return asyncio.run(my_async_agent.process(input_text))
```

### A2. Summary Output Examples

| Phase | Example |
|-------|---------|
| Login | `✅ Login → user@example.com` |
| Project | `✅ Project → "my-bot" (proj_123) 🔗 .../simulate/scenarios?project=proj_123` |
| Data | `✅ Data → 3 files uploaded to project library` |
| Scenario | `✅ Scenario → "Happy Path" (scn_456) 🔗 .../simulate/scenarios/scn_456?project=proj_123` |
| Contracts | `✅ Contracts → 3개 생성됨 🔗 .../simulate/scenarios/scn_456?project=proj_123` |
| Input Set | `✅ Input Set → inp_789 (10 inputs) 🔗 .../simulate/scenarios/scn_456/inputs/inp_789?project=proj_123` |
| QC | `✅ QC → format: 10/10, duplicates: 0, diversity: high` |
| Bundle | `✅ Bundle → v1 (bnd_012) 🔗 .../simulate/scenarios/scn_456/bundles/bnd_012?project=proj_123` |
| Test | `✅ Test → exp_abc (10 runs) 🔗 .../evaluate/experiments/exp_abc?project=proj_123` |
| Eval | `✅ Evaluation → 3 insights 🔗 .../evaluate/experiments/exp_abc?project=proj_123` |
