---
name: fluxloop-test
description: |
  Use for running tests against scenarios — includes data selection, bundle management, and test execution.
  Frequency: the most common entry point. Run repeatedly during the test-evaluate-fix loop.
  Keywords: test, run test, test my agent, simulation, run simulation, generate test data, synthesize inputs

  Auto-activates on requests like:
  - "run the test", "test my agent"
  - "run simulation", "execute test"
  - "generate test data", "synthesize inputs"
---

# FluxLoop Test Skill

**Select-Check-Execute**: Data selection → Pre-check (every path) → Test execution → Result recording (Dual Write)

## Output Format

> 📎 All user-facing output must follow: read skills/_shared/OUTPUT_FORMAT.md

## Context Protocol

1. `fluxloop context show` → confirm project + scenario are set up
2. `.fluxloop/test-memory/` check:
   - Load `agent-profile.md` → stale detection (compare `git_commit` vs `git rev-parse --short HEAD`)
   - Load `test-strategy.md` → understand test objectives and config
   - Missing files → proceed (first run); if `agent-profile.md` missing, guide to context skill
3. Dual Write:
   - Server: `fluxloop test --scenario <name>` (results stored on server, experiment ID generated)
   - Local: append to `.fluxloop/test-memory/results-log.md`
4. On completion: verify `results-log.md` has the new entry for the evaluate skill

> 📎 Stale detection: read skills/_shared/CONTEXT_PROTOCOL.md
> 📎 Collection procedure (for inline refresh): read skills/_shared/CONTEXT_COLLECTION.md

## Prerequisite

Run `fluxloop context show` first:
- ✅ Project selected + scenario exists → proceed
- ❌ Missing steps detected → Prerequisite Resolution (📎 read skills/_shared/PREREQUISITE_RESOLUTION.md):
  - Identify the missing scope and list the required chain:
    - setup missing: "setup → context → scenario are required. Proceed in order?"
    - context missing: "context → scenario are required. Proceed in order?"
    - scenario only missing: "Scenario creation is required. Proceed first?"
  - Approved: run required skills in order inline → on each completion "✅ {skill_name} complete." → after all complete, return to Step 1
  - Denied: stop

## Workflow

### Step 1: Context Load

- Read `.fluxloop/test-memory/agent-profile.md`:
  - Stale detection (compare `git_commit` vs `git rev-parse --short HEAD`)
  - If stale → "The profile looks outdated. Would you like to update it?"
    - Yes → follow `_shared/CONTEXT_COLLECTION.md` procedure inline
    - No → continue with existing profile
  - If `git_commit` is `no-git` → continue without warning
  - Understand agent characteristics (tools, features, dependencies)
- Read `.fluxloop/test-memory/test-strategy.md` (if exists):
  - Understand test objectives, evaluation criteria, test configuration
  - Use wrapper path info from test-strategy in Step 3 pre-check
- If `test-strategy.md` missing: "test-strategy.md not found. You can still run a test, but running the scenario skill first provides better context."

### Step 2: Bundle/Input Selection

> 💡 **Terminology** (must be explained to the user on first appearance in the session):
> - **Bundle**: A snapshot that combines test inputs and personas. Enables repeatable tests under identical conditions.
> - **Input Set**: A collection of AI-generated test input data. Must be published as a Bundle before it can be used in tests.
> - **Persona**: A virtual user type for testing (e.g., "impatient customer", "first-time user").

> 📎 Bundle selection decision tree: read skills/_shared/BUNDLE_DECISION.md

Run `fluxloop bundles list --scenario-id <id>` and follow the decision tree:

| Bundle State | Input Set State | Path |
|--------------|----------------|------|
| Multiple | - | User selects → Step 3 |
| One | - | "Use existing / Create new?" → Step 3 or generation |
| None | Multiple | User selects → `bundles publish` → Step 3 |
| None | One | "Use existing / Create new?" → Step 3 or generation |
| None | None | Full generation → Step 3 |

When showing multiple resources, include: **version/name, tag/description, count, created date**

**Full generation path (must output results to the user after each step):**

1. Persona generation:
   ```bash
   fluxloop personas suggest --scenario-id <id>
   ```
   > **Required output**: `✅ Personas → N generated` + list of generated persona names

2. Input generation:
   ```bash
   fluxloop inputs synthesize --scenario-id <id>    # --timeout 300 for large, --total-count 2 for quick
   ```
   > **Required output**: Extract `input_set_id` and input count from CLI output:
   > `✅ Input Set → {input_set_id} ({N} inputs) 🔗 https://alpha.app.fluxloop.ai/simulate/scenarios/{scenario_id}/inputs/{input_set_id}?project={project_id}`
   > + brief summary of generated inputs (1-2 lines describing what test cases were created)

3. (Interactive only) QC & Refine:
   ```bash
   fluxloop inputs qc --scenario-id <id> --input-set-id <id>
   fluxloop inputs refine --scenario-id <id> --input-set-id <id>
   ```

4. Bundle publish:
   ```bash
   fluxloop bundles publish --scenario-id <id> --input-set-id <id>
   ```
   > **Required output**: `✅ Bundle → v{N} ({bundle_version_id}) 🔗 https://alpha.app.fluxloop.ai/simulate/scenarios/{scenario_id}/bundles/{bundle_version_id}?project={project_id}`

> ⚠️ Do NOT run `fluxloop test` here — always proceed to Step 3 first. (E-M2 fix)

### Step 3: Pre-check (mandatory for ALL paths)

This step ensures no path skips essential checks. (L-H1 fix)

1. **Wrapper check**: Verify `.fluxloop/scenarios/<name>/agents/wrapper.py` or `runner.target` in `configs/simulation.yaml`
   - Not configured → "Wrapper setup is needed. See the scenario skill's wrapper guide."
2. **Turn mode selection**: "Multi-turn? (yes/no), max turns? (default: 8)"
   > 💡 **What is Multi-turn?** Simulates a conversation with multiple back-and-forth exchanges with the agent. Single-turn tests only one question-response, while Multi-turn verifies the ability to maintain context across consecutive conversations.
   - If `test-strategy.md` has previous settings → suggest as default
3. **Provider selection** (multi-turn only): "Provider? (openai/anthropic)"

### Step 4: Data Sync

```bash
fluxloop sync pull --bundle-version-id <id>
```

> ⚠️ Never use `--pull` option — `sync pull` and `test` must run as separate commands.

### Step 5: Test Execution (Dual Write)

**(Server)**:

Single-turn:
```bash
fluxloop test --scenario <name>
```

Multi-turn:
```bash
! fluxloop test --scenario <name> --multi-turn --max-turns <N>
```

> 📎 Multi-turn rules: read skills/_shared/MULTITURN.md

**(Local)**: Append to `.fluxloop/test-memory/results-log.md` (format: `test-memory-template/results-log.md`)

Fields to populate:
- Date, scenario name
- Experiment ID: `exp_xxx`
- Bundle: `bundle_version_id`
- Input count: N
- Turn mode: single / multi-turn
- Pass / Fail: N / M
- Duration: Ns
- Evaluation Results: (leave empty — evaluate skill fills this)
- Insight: (leave empty — evaluate skill fills this)
- Server link: experiment URL

> **Required link output**: After test completion, extract `experiment_id` from CLI output and display:
> `✅ Test → exp_xxx (N runs) 🔗 https://alpha.app.fluxloop.ai/release/experiments/{experiment_id}/evaluation?project={project_id}`

### Step 6: Results Review

```bash
fluxloop test results --scenario <name>
```

Display result summary to the user.

> 📎 CLI options: read skills/test/references/cli-commands.md

## Error Handling

| Error | Response |
|-------|----------|
| No project set up | Apply Prerequisite Resolution → suggest inline setup~scenario chain execution |
| No scenario | Apply Prerequisite Resolution → suggest inline scenario execution |
| `Sync API key not set` | "Run `fluxloop apikeys create` or check `.fluxloop/.env`" |
| `Inputs file not found` | "Run `fluxloop sync pull --bundle-version-id <id>` first" |
| `No personas found` | "Run `fluxloop personas suggest --scenario-id <id>` first" |
| `Synthesis timed out` | "Use `--timeout 300` or reduce `--total-count`" |
| `ModuleNotFoundError` in test | "Check `runner.target` in simulation.yaml, ensure wrapper is in Python path" |
| Agent returns None | "Ensure wrapper returns string, not None" |

## Next Steps

Test complete. Available next actions:
- Analyze results and generate insights (evaluate skill)
- Compare prompt versions A vs B (prompt-compare skill)

## Quick Reference

| Step | Command |
|------|---------|
| Check | `fluxloop context show` |
| Bundles | `fluxloop bundles list --scenario-id <id>` |
| Inputs | `fluxloop inputs list --scenario-id <id>` |
| Personas | `fluxloop personas suggest --scenario-id <id>` |
| Synthesize | `fluxloop inputs synthesize --scenario-id <id>` |
| QC | `fluxloop inputs qc --scenario-id <id> --input-set-id <id>` |
| Refine | `fluxloop inputs refine --scenario-id <id> --input-set-id <id>` |
| Publish | `fluxloop bundles publish --scenario-id <id> --input-set-id <id>` |
| Sync | `fluxloop sync pull --bundle-version-id <id>` |
| Test | `fluxloop test --scenario <name>` |
| Multi-turn | `! fluxloop test --scenario <name> --multi-turn --max-turns <N>` |
| Results | `fluxloop test results --scenario <name>` |
| Git hash | `git rev-parse --short HEAD` |

> 📎 Full CLI reference: read skills/_shared/QUICK_REFERENCE.md

## Key Rules

1. Always read `agent-profile.md` first — check stale detection before proceeding
2. Read `test-strategy.md` (if exists) for test objectives and configuration
3. NEVER skip Step 3 (Pre-check) regardless of which bundle path was taken (L-H1 fix)
4. NEVER run `fluxloop test` as part of bundle selection (E-M2 fix) — always go through Pre-check first
5. Use `sync pull` + `test` separately — NEVER use `--pull` option
6. Multi-turn commands must start with `!` prefix
7. Use explicit IDs (`--bundle-version-id`, `--scenario-id`) — **CLI table output may truncate UUIDs. Always validate 36 characters (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`) before use. If less than 36 characters, re-run the list command to obtain the full ID.**
8. Dual Write: server (test results + experiment ID) and local (`results-log.md`) at the same time
9. Use the template from `test-memory-template/results-log.md` for output format
10. Append to `results-log.md` (most recent at top) — do NOT overwrite
