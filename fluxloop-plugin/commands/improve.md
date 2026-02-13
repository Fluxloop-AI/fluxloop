---
description: Analyze test results, fix agent code, and re-test (iteration loop)
allowed-tools: [Bash, Read, Edit, Write, Glob, Grep]
---

# FluxLoop Improve

Analyze test results → fix agent code → re-test → re-evaluate (Phase 6 iteration loop).

## Pre-check

```bash
fluxloop context show          # Ensure scenario + experiment exist
fluxloop test results --scenario <name>   # Must have prior test results
```

## Flow

### 1. Sync & Analyze

```bash
fluxloop sync pull --bundle-version-id <id>
fluxloop test results --scenario <name>
```

Analyze results:
- Count warning turns and failure patterns
- Identify contract violations
- Map issues to specific code locations (file:line)

### 2. Fix Agent Code

- Suggest fixes based on analysis
- **Always require user confirmation before applying changes** (even in Auto mode)
- After edit, smoke test runs automatically: `fluxloop test --smoke --quiet`

### 3. Re-test (same bundle)

```bash
fluxloop sync pull --bundle-version-id <id>   # Same bundle for fair comparison
fluxloop test --scenario <name>
```

### 4. Re-evaluate

```bash
fluxloop evaluate --experiment-id <new_id> --wait
```

Output:
```
✅ Re-evaluation → N insights 🔗 https://alpha.app.fluxloop.ai/evaluate/experiments/exp_new?project=proj_123
📋 Compare with the previous baseline in the web app
💡 Run again to continue improving
```

## Loop

Unsatisfied with results → repeat from step 1.

## Notes

- Code changes **always** require user confirmation, even in Auto mode
- Re-test uses the **same bundle** to ensure fair before/after comparison
- Turn streaming and auto-upload are enabled by default — real-time monitoring is available in the web app
