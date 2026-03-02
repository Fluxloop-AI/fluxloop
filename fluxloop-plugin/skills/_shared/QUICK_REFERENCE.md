# CLI Quick Reference

## Execution Mode

Ask once at the start of a session:

| Mode | Description |
|------|-------------|
| 🚀 **Auto** | Run all phases without stopping, only pause on errors |
| ✅ **Interactive** (default) | Pause after each major step for confirmation |

Prompt: `"Run automatically or step-by-step? (auto/interactive, default: interactive)"`

## Terminology

| Term | Description |
|------|-------------|
| **Web Project** | Remote project on FluxLoop cloud (`project_id`) |
| **Web Scenario** | Remote scenario on FluxLoop cloud (`scenario_id`) |
| **Local Scenario** | Local folder at `.fluxloop/scenarios/<name>/` |
| **Input Set** | Generated test inputs (`input_set_id`) |
| **Bundle** | Published snapshot of inputs + personas (`bundle_version_id`) |

## Workspace Structure

```
project_root/
  .fluxloop/
    project.json          # Web Project connection
    context.json          # Current scenario pointer
    .env                  # API key (shared by all scenarios)
    scenarios/
      scenario-a/
        .env              # Optional scenario-local env
        agents/           # Agent wrapper files
        configs/          # simulation.yaml, evaluation.yaml
        .state/
          contracts/      # Scenario contracts (YAML)
        inputs/           # Generated test inputs
        experiments/      # Test results
    test-memory/            # Shared context across skills
      agent-profile.md
      test-strategy.md
      prompt-versions.md
      results-log.md
      learnings.md
```

## ID Extraction

Always use `--format json` on list commands that require IDs. Table output may truncate UUIDs depending on terminal width.

```bash
fluxloop bundles list --scenario-id <id> --format json
fluxloop inputs list --scenario-id <id> --format json
```

## Setup

| Command | Description |
|---------|-------------|
| `fluxloop context show` | Check current state |
| `fluxloop auth login` | Login (`--staging` available) |
| `fluxloop projects select <id>` | Select project (`--staging` available) |

## Context

| Command | Description |
|---------|-------------|
| `fluxloop data push <file>` | Upload file (`--bind` to link to scenario) |
| `fluxloop data push <file> --usage ground-truth --scenario <id>` | Upload as Ground Truth validation data (auto-materializes) |
| `fluxloop data bind <data_id> --role validation --scenario <id>` | Bind existing data as GT validation (auto-materializes) |

## Scenario

| Command | Description |
|---------|-------------|
| `fluxloop init scenario <name>` | Create local scenario folder |
| `fluxloop scenarios create --name X --goal "..."` | Create web scenario |
| `fluxloop scenarios select <id>` | Select scenario (auto-detects local folder) |
| `fluxloop scenarios select <id> --local-path <folder>` | Select with explicit local folder |
| `fluxloop apikeys create` | Create API key (saved to `.fluxloop/.env`) |

## Data

| Command | Description |
|---------|-------------|
| `fluxloop bundles list --scenario-id <id>` | List existing bundles |
| `fluxloop personas suggest --scenario-id <id>` | Generate stories + cast personas (full mode) |
| `fluxloop personas suggest --scenario-id <id> --stories '<json>'` | Cast-only mode — agent provides stories, server skips story generation and casts only |
| `fluxloop inputs synthesize --scenario-id <id>` | Generate inputs (`--timeout 300` for large, 409 data-context conflicts require retry after readiness) |
| `fluxloop bundles publish --scenario-id <id> --input-set-id <id>` | Publish bundle |
| `fluxloop sync pull --bundle-version-id <id>` | Download bundle (uses current scenario) |
| `fluxloop inputs qc --scenario-id <id> --input-set-id <id>` | Input quality check (interactive only) |
| `fluxloop inputs refine --scenario-id <id> --input-set-id <id>` | AI-based input refinement (may return same 409 data-context conflicts) |

## Ground Truth

| Command | Description |
|---------|-------------|
| `fluxloop data push <file> --usage ground-truth --scenario <id>` | Upload + bind as GT validation data |
| `fluxloop data push <file> --usage ground-truth --bind --label-column <col> --split test` | Upload GT with label column and split |
| `fluxloop data bind <data_id> --role validation --scenario <id>` | Bind existing data as GT validation |
| `fluxloop data bind <data_id> --role validation --split test --label-column <col>` | Bind with GT options |
| `fluxloop data gt status --scenario <id>` | Check GT materialization status |
| `fluxloop data gt status --scenario <id> --data-id <id>` | Check GT status for specific data |
| `fluxloop data gt status --scenario <id> --format json` | GT status in JSON format |

> 📎 For GT classification guidance (context vs ground-truth): read skills/context/SKILL.md Step 5-1

## Test

| Command | Description |
|---------|-------------|
| `fluxloop test --scenario <name>` | Run test |
| `! fluxloop test --scenario <name> --multi-turn` | Multi-turn test |
| `! fluxloop test --scenario <name> --multi-turn --max-turns 5` | Multi-turn (max 5 turns) |
| `fluxloop test results --scenario <name>` | Check latest results |

## Evaluate

| Command | Description |
|---------|-------------|
| `fluxloop evaluate --experiment-id <id> --wait` | Trigger evaluation + show insights |

## Compare (Phase-by-Phase Flow)

| Phase | Action | Command |
|-------|--------|---------|
| 0 | Check context | `fluxloop context show` |
| 1 | Select bundle | `fluxloop bundles list --format json`, `sync pull --bundle-version-id <id>` |
| 2 | Configure | Edit `simulation.yaml`, decide multi-turn |
| 3 | Run baseline | `git diff HEAD` + `fluxloop test --scenario <name>` |
| 4 | Modify prompt | (user action) |
| 5 | Run variant | `git diff HEAD` + `fluxloop test --scenario <name>` |
| 6 | Compare analysis | `trace_summary.jsonl` × 2 + git diffs → report |
| 7 | Next step | Repeat / evaluate / done |
