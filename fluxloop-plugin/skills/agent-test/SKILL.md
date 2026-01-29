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

**Context-First:** Always read context first → understand state → ask user → execute after confirmation

```
1. Check context (fluxloop context show)
2. Summarize current state
3. Present options (NO auto-execution)
4. Execute after user confirmation
5. Save results to context
```

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
        inputs/           # Generated test inputs
        experiments/      # Test results
      scenario-b/
        ...
```

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

# 2. Select or create project
fluxloop projects list
fluxloop projects select <project_id>
# OR
fluxloop projects create --name "my-agent"
fluxloop intent refine --intent "..."
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
fluxloop init scenario order-bot

# 2. Create and refine web scenario
fluxloop scenarios create --name "Order Bot" --goal "..."
fluxloop scenarios refine --scenario-id <id>

# 3. Create API key (saves to .fluxloop/.env, shared by all scenarios)
fluxloop apikeys create

# 4. Set up agent wrapper (if complex agent - see "Agent Wrapper Setup" section)
#    Create: .fluxloop/scenarios/<name>/agents/wrapper.py
#    Update: configs/simulation.yaml → runner.target: "agents.wrapper:run"
```

> **Note:** API keys are project-scoped and stored in `.fluxloop/.env`.
> All scenarios in the workspace share the same API key.

**Common mistake:** Running `init scenario` from home directory creates in `~/.fluxloop/` instead of workspace.

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

**Full generation (5 commands):**
```bash
fluxloop personas suggest --scenario-id <id>
fluxloop inputs synthesize --scenario-id <id>  # Use --timeout 300 for large sets
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

---

## Agent Wrapper Setup

To run tests, FluxLoop needs to invoke your agent.
The `runner.target` in `configs/simulation.yaml` must point to the agent entry point.

### When is Wrapper Needed?

| Agent Type | Wrapper? | Description |
|------------|----------|-------------|
| Simple function `def run(input: str) -> str` | ❌ | Direct call |
| Class/stateful agent | ✅ | Wrap initialization |
| External dependencies (DB, broker, API) | ✅ | Wrap dependency injection |
| Needs conversation_id or context | ✅ | Wrap metadata handling |

### Setup Steps

1. Create wrapper: `.fluxloop/scenarios/<name>/agents/wrapper.py`
2. Update `configs/simulation.yaml` → `runner.target: "agents.wrapper:run"`
3. Run test

### Wrapper Template

```python
# agents/wrapper.py
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

### Async Wrapper

```python
def run(input_text: str, metadata: dict = None) -> str:
    return asyncio.run(my_async_agent.process(input_text))
```

### Debug

```bash
cd .fluxloop/scenarios/<name>
python -c "from agents.wrapper import run; print(run('test'))"
```

---

## Quick Reference

| Command | Description |
|---------|-------------|
| `fluxloop context show` | Check current state |
| `fluxloop auth login` | Login |
| `fluxloop projects select <id>` | Select project |
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
| `fluxloop test results --scenario <name>` | View latest test results |
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

1. **Always check context first** (`fluxloop context show`)
2. **Check existing data with list** (`bundles list`, `inputs list`)
3. **Ask user before executing** (NO auto-execution)
4. **Run sync pull + test separately** (Do NOT use `--pull`)
5. **Use explicit IDs** (`--bundle-version-id`, `--scenario-id`)
6. **Use evaluate --wait for insights** (Partial jobs can still yield headlines)
7. **Complex agents need wrapper** (See "Agent Wrapper Setup" section)
