---
name: fluxloop-setup
description: |
  Use for FluxLoop CLI installation, authentication, and project setup.
  Frequency: once per project. Automatically skipped if already set up.
  Keywords: setup, install, configure, login, auth, first time, get started

  Auto-activates on requests like:
  - "set up fluxloop", "install fluxloop"
  - "configure fluxloop", "log in to fluxloop"
  - "get started"
---

# FluxLoop Setup Skill

**Setup-First**: Check state → Install → Authenticate → Select project → Guide next step

## Output Format

> 📎 All user-facing output must follow: read skills/_shared/OUTPUT_FORMAT.md

## Context Protocol

> setup is a pre-test-memory step — no reads, no writes.

1. `fluxloop context show` → check current state (determine if setup is already complete)
2. If already complete → guide to context or scenario skill

> 📎 Full protocol: read skills/_shared/CONTEXT_PROTOCOL.md

## Prerequisite

None — this is the entry point. No prior skills required.

## Workflow

### Step 1: CLI Installation Check

Check if installed first — **this must run before any other `fluxloop` command**:

```bash
fluxloop --version
```

If not installed (`command not found`), guide based on the workspace environment:

| Environment | Install Command | Run Command |
|-------------|----------------|-------------|
| uv | `uv add fluxloop-cli` | `uv run fluxloop ...` |
| pip | `pip install fluxloop-cli` | `fluxloop ...` (venv activated) |
| poetry | `poetry add fluxloop-cli` | `poetry run fluxloop ...` |

**Priority**: Install in the SAME environment where the agent runs — this is critical for simulation.

#### Dependency Safety Guardrails

1. Install `fluxloop-cli` only (NOT `fluxloop`)
2. Before install/upgrade, run `python --version` and `uv pip index versions fluxloop-cli`
3. If the target version is not published, report available versions and **stop** (never guess ranges)
4. If Python requirement is higher than the current interpreter, **stop** and offer two choices: upgrade Python or pin a compatible version
5. On failure, explain root cause in one line (name/version/python mismatch)

### Step 2: Context Check

After CLI is confirmed installed, determine current state:

```bash
fluxloop context show
```

| State | Next Action |
|-------|-------------|
| No context.json | Step 3 (authentication) |
| Auth only (no project) | Step 4 (project selection) |
| Project selected | "Setup complete. Next: codebase scan & agent profile creation (context skill)." |
| Scenario exists | "Scenario ready. Next: run tests against the scenario (test skill)." |

Also run `fluxloop auth status` to verify login state.

### Step 3: Authentication

```bash
fluxloop auth login --no-wait && fluxloop auth login --resume
```

These two commands form a single flow:
- `--no-wait` prints the device code for the user to enter
- `--resume` polls until the login completes

> 📎 Staging environment: read skills/_shared/STAGING.md

### Step 4: Project Selection

List existing projects:

```bash
fluxloop projects list --format json
```

**If a project exists**: select it:

```bash
fluxloop projects select <project_id>
```

**If creating a new project**:

#### Naming Rules

| Field | Language | Format |
|-------|----------|--------|
| Folder name | **English only** | kebab-case (`order-bot`) |
| Project/Scenario name | Any | Display text ("Order Bot", "Support Agent") |

Suggest 3 candidates based on the codebase:
- Analyze: `package.json`, `pyproject.toml`, main agent file, README, directory name
- Folder: keywords → kebab-case → suffix (`-agent`, `-bot`)
- Display: concise, user-friendly names

```
📁 Folder: 1) order-bot  2) food-agent  3) cs-service
📝 Name: 1) Order Bot  2) Support Bot  3) Food Agent
Select number or type custom:
```

#### Language Selection

Ask the user for the default language **once**:

```
🌐 Language: 1) en (English)  2) ko (Korean)  3) ja (Japanese)  4) other
Select number or type language code (default: en):
```

Create the project:

```bash
fluxloop projects create --name "my-agent" --language <code>
```

> **Required link output**: After project creation/selection, extract `project_id` from CLI output and display:
> `✅ Project → "{name}" (proj_xxx) 🔗 https://alpha.app.fluxloop.ai/simulate/scenarios?project={project_id}`

## Error Handling

| Error | Response |
|-------|----------|
| `fluxloop: command not found` | Guide to Step 1 (installation) |
| Auth failure / token expired | Guide to Step 3 (authentication) |
| `fluxloop-cli` version mismatch | Follow Dependency Safety Guardrails — report exact available versions |
| Project creation failure | Check network, verify login status |

## Next Steps

Setup complete. Available next actions:
- Codebase scan & agent profile creation (context skill)
- Scenario creation (scenario skill — auto-runs context if no profile exists)

## Quick Reference

| Step | Command |
|------|---------|
| Install check | `fluxloop --version` |
| State check | `fluxloop context show` |
| Auth check | `fluxloop auth status` |
| Login | `fluxloop auth login --no-wait && fluxloop auth login --resume` |
| Projects | `fluxloop projects list --format json` |
| Projects | `fluxloop projects select <id>` |
| Projects | `fluxloop projects create --name X --language <code>` |

> 📎 Full CLI reference: read skills/_shared/QUICK_REFERENCE.md

## Key Rules

1. Always check `fluxloop --version` first — if not installed, start from Step 1 (installation)
2. After CLI confirmed, run `fluxloop context show` — skip to the right step based on state
3. Install `fluxloop-cli` (NOT `fluxloop`)
4. Install in the SAME environment where your agent runs
5. Never guess package versions — check `uv pip index versions fluxloop-cli`
6. Folder names: English kebab-case only (`order-bot`)
7. Project/scenario display names: any language allowed
8. Suggest 3 naming candidates, allow custom input
9. Ask language once at project creation (default: `en`)
10. After setup, guide to context skill — never jump to scenario directly
