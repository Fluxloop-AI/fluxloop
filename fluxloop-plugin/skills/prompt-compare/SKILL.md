---
name: fluxloop-prompt-compare
description: |
  Use for prompt version comparison and stability testing.
  Keywords: compare, comparison, prompt version, stability, A/B test, diff

  Auto-activates on requests like:
  - "compare prompts", "compare prompt versions"
  - "stability test", "run a stability test"
  - "v3 vs v4", "version comparison"
  - "run same input multiple times", "run the same input multiple times"
---

# FluxLoop Prompt Compare Skill

Compare prompt versions by running the same bundle multiple times and analyzing output differences.

## Core Principle

**Same Bundle × N Repeats × Version Diff** — freeze inputs via bundle, automate repeated runs, compare outputs.

## Prerequisite

Requires a FluxLoop scenario to exist (agent-test Phase 1~2 completed).
If no scenario exists, guide: `"Scenario setup is required first. Start with 'test my agent'."`

> **Staging:** if the user mentions "staging", prefer the `--staging` flag during setup.
> (avoid `--api-url` unless a custom domain is required)
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

> **Tip:** For comparison tests, 1-3 inputs are usually enough. When creating new data, use `--total-count 2` for a small bundle.

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
1. Number of repeats? (default: 5)
2. Multi-turn? (default: single-turn) → if yes, also confirm max turns
3. Current prompt version label? (e.g., "v3", "current version")
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
3. Output: `✅ Baseline → exp_<timestamp> (label: "v3", N runs)`

---

## Phase 4: Prompt Modification

```
Please update the prompt.
Let me know when you're done, and share the new version label (e.g., "v4").
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
2. Output: `✅ Variant → exp_<timestamp> (label: "v4", N runs)`

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
## Prompt Changes (v3 -> v4)
- [Summary of changed files and key edits]
```

#### 2) Per-Input Analysis

Group traces by `input` field, then compare across versions:

```markdown
## Input: "I want to analyze data"

### v3 (5 runs)
| # | Output Summary | Tokens | Time (ms) |
|---|----------|------|----------|
| 1 | [One-line key summary] | 150 | 1200 |
| 2 | ... | ... | ... |

### v4 (5 runs)
| # | Output Summary | Tokens | Time (ms) |
|---|----------|------|----------|
| 1 | [One-line key summary] | 130 | 1100 |
| 2 | ... | ... | ... |

### Comparison
- **Within-version consistency**: v3 high / v4 medium
- **Cross-version difference**: clear / minimal
- **Key change**: [What changed concretely]
```

#### 3) Overall Summary

```markdown
## Overall Comparison

| Metric | v3 | v4 |
|------|----|----|
| Avg token count | 150 | 130 |
| Avg response time (ms) | 1200 | 1100 |
| Success rate | 100% | 100% |
| Within-version consistency | High | Medium |

### Conclusion
[Summarize impact by linking prompt changes to result changes]
```

---

## Phase 7: Next Actions

```
Choose one:
1. Additional comparison — update prompt again and compare (-> Phase 4)
2. View Web details — provide the raw experiment URL (`https://...`) so it can be copied directly
3. Server evaluation — run detailed analysis with `fluxloop evaluate`
4. Done
```

If "Additional comparison": loop back to Phase 4 (same bundle reused).
If "Server evaluation":
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
