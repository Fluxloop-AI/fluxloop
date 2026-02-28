---
name: fluxloop-evaluate
description: |
  Use for evaluating test results, analyzing insights, and improving the agent.
  Frequency: after every test run. Core of the daily test → evaluate → fix loop.
  Keywords: evaluate, evaluation, improve, analyze results, insights, recommendations, re-test

  Auto-activates on requests like:
  - "evaluate the results", "analyze results"
  - "improve my agent", "what went wrong"
  - "generate insights", "review test results"
---

# FluxLoop Evaluate Skill

**Evaluate-Analyze-Improve**: Server evaluation → Result analysis → Insight recording → Code improvement → Re-test loop

## Output Format

> 📎 All user-facing output must follow: read skills/_shared/OUTPUT_FORMAT.md

## Context Protocol

1. `fluxloop context show` → confirm project / scenario / test results exist
2. `.fluxloop/test-memory/` check:
   - Exists → load `agent-profile.md`, `results-log.md`, `test-strategy.md`
   - Missing → "Run 'run the test' (test skill) first"
3. Dual Write:
   - Server: `fluxloop evaluate --experiment-id`
   - Local: save to `.fluxloop/test-memory/learnings.md`, append to `.fluxloop/test-memory/results-log.md`
4. On completion: verify `learnings.md` is current (next scenario skill reads it)

> 📎 Full protocol: read skills/_shared/CONTEXT_PROTOCOL.md
> 📎 Collection procedure: read skills/_shared/CONTEXT_COLLECTION.md

## Prerequisite

Run `fluxloop context show` first:
- ✅ Project selected + scenario exists + test completed → proceed
- ❌ Missing steps detected → Prerequisite Resolution (📎 read skills/_shared/PREREQUISITE_RESOLUTION.md):
  - Identify the missing scope and list the required chain:
    - setup missing: "setup → context → scenario → test are required. Proceed in order?"
    - context missing: "context → scenario → test are required. Proceed in order?"
    - scenario missing: "scenario → test are required. Proceed in order?"
    - test only missing: "Test execution is required. Proceed first?"
  - Approved: run required skills in order inline → on each completion "✅ {skill_name} complete." → after all complete, return to Step 1
  - Denied: stop

Verify test completion: check `.fluxloop/test-memory/results-log.md` has at least 1 entry, or run `fluxloop test results --scenario <name>`.

## Workflow

### Step 1: Context Load

- Read `.fluxloop/test-memory/agent-profile.md` → stale check (compare `git_commit` vs `git rev-parse --short HEAD`)
  - Stale → "The profile appears outdated. Would you like to update it?"
    - Yes → run collection procedure inline (📎 read skills/_shared/CONTEXT_COLLECTION.md)
    - No → continue with existing profile
- Read `.fluxloop/test-memory/results-log.md` → identify latest experiment ID, pass/fail ratio, history
- Read `.fluxloop/test-memory/test-strategy.md` → load Evaluation Criteria

### Step 2: Server Evaluation

Trigger server-side evaluation:

```bash
fluxloop evaluate --experiment-id <id> --wait
fluxloop evaluate --experiment-id <id> --wait --timeout 900 --poll-interval 5
```

- `--wait` polls until status is `completed`, `partial`, `failed`, or `cancelled`
- When finished as `completed` or `partial` with at least one run completed → insights are generated
- If job stays `queued` >30s without `locked_at` → warn: "Workers may be down or backlog is high"

**Dual Write**:
- (Server) Evaluation results stored on server
- (Local) Append "Evaluation Results" section to the matching experiment entry in `.fluxloop/test-memory/results-log.md`:

```markdown
### Evaluation Results (added after evaluate skill runs)

| Criterion | Score | Notes |
|-----------|-------|-------|
| {criterion name} | {score} | {brief comment} |

**Insight**: {key finding from this run}

**Server link**: 🔗 {experiment URL}
```

> **Required link output**: Must follow the Web Handoff format below. Extract `experiment_id` and `project_id` from CLI output to construct the URL.

**Web Handoff** — output after evaluation:

```
✅ Evaluation → N insights 🔗 https://alpha.app.fluxloop.ai/release/experiments/exp_abc/evaluation?project=proj_123
📋 Check detailed analysis in the web app:
  - Decision: gates, budgets, baseline comparison
  - Insights: findings by category (with severity)
  - Recommendations: improvement suggestions (with priority)
  - Baseline: set the current result as baseline
```

### Step 3: Result Analysis + Insight Recording

> 📎 Bundle selection required. If no bundle is selected: read skills/_shared/BUNDLE_DECISION.md

Sync and analyze:

```bash
fluxloop sync pull --bundle-version-id <id>
fluxloop test results --scenario <name>
```

Analyze: failure patterns, warning turns, contract violations, related code locations.

Save findings to `.fluxloop/test-memory/learnings.md`:
- Format: follow `test-memory-template/learnings.md`
- **Discovered Patterns**: add Strengths / Weaknesses with new evidence
- **Applied Improvements**: add new rows to the table
- **Warnings for Next Test**: issues requiring verification
- **Open Questions**: unanswered hypotheses
- If existing content → update (preserve existing + add new evidence), never overwrite

This `learnings.md` is read by the scenario skill in the next cycle → **core of the improvement loop**.

### Step 4: Code Fix Suggestions

Based on analysis, suggest agent code modifications with specific file:line references.

> **Code changes always require user confirmation**, even in Auto mode.

| Step | Interactive | Auto |
|------|------------|------|
| Result analysis | Show + confirm | Show only |
| Code fix | Suggest → confirm (required) | Suggest → confirm (required) |
| Re-test | Ask "Re-test?" | Auto-proceed |
| Re-evaluate | Ask "Re-evaluate?" | Auto-proceed |

After edits, the hook runs `fluxloop test --smoke --quiet` automatically.

### Step 5: Re-test Guidance

If code was modified → guide to re-test:
- "run the test" (test skill)
- Use the same bundle for comparison consistency:
  ```bash
  fluxloop sync pull --bundle-version-id <id>
  fluxloop test --scenario <name>
  ```

> 📎 Bundle selection required. If no bundle is selected: read skills/_shared/BUNDLE_DECISION.md

### Step 6: Re-evaluate (Iteration Loop)

If unsatisfied with re-test results → repeat from Step 2:
- Run the evaluate workflow again for the same flow
- Each iteration appends to `learnings.md` Applied Improvements → tracks improvement history

## Error Handling

| Error | Response |
|-------|----------|
| Experiment ID not found | `fluxloop test results --scenario <name>` to find recent experiment ID |
| Evaluation timeout | Retry with `--timeout 900 --poll-interval 5` |
| Evaluation status `failed` / `cancelled` | Possible server issue; check status in web app |
| `.fluxloop/test-memory/results-log.md` missing | Apply Prerequisite Resolution → suggest inline test execution |
| Worker delay (`queued` >30s without `locked_at`) | "Workers may be down or backlog is high" |

## Next Steps

Evaluation complete. Available next actions:
- Re-test with updated code (test skill)
- Compare prompt versions A vs B (prompt-compare skill)
- Improve scenario using learnings (scenario skill — reads learnings.md)

## Quick Reference

| Step | Command |
|------|---------|
| Check | `fluxloop context show` |
| Results | `fluxloop test results --scenario <name>` |
| Evaluate | `fluxloop evaluate --experiment-id <id> --wait` |
| Sync | `fluxloop sync pull --bundle-version-id <id>` |

> 📎 Full CLI reference: read skills/_shared/QUICK_REFERENCE.md

## Key Rules

1. Always run `fluxloop context show` first — route to the correct step based on state
2. Stale check `agent-profile.md` on load (`git_commit` comparison)
3. Dual Write: server evaluation results + local `results-log.md` update simultaneously
4. Code changes always require user confirmation (Auto mode included — no exceptions)
5. `learnings.md`: update (preserve existing + add new evidence); `results-log.md`: append
6. Re-test / re-evaluate loop: repeat Steps 2-6 until satisfied
7. Cite specific file:line locations when suggesting code improvements
