---
name: fluxloop-context
description: |
  Use for scanning the codebase and creating/updating the agent profile.
  Frequency: once initially, then only when agent code changes. Stale detection handles this automatically.
  Keywords: context, profile, scan, update profile, agent info

  Auto-activates on requests like:
  - "scan the agent", "analyze the agent"
  - "update profile", "refresh profile"
  - "what does this agent do?"
---

# FluxLoop Context Skill

**Scan-Summarize-Save**: Codebase scan → Profile summary → Server upload + Local save (Dual Write)

## Output Format

> 📎 All user-facing output must follow: read skills/_shared/OUTPUT_FORMAT.md

## Context Protocol

1. `fluxloop context show` → confirm project is set up
2. `.fluxloop/test-memory/` check:
   - Exists → load `agent-profile.md` (show existing profile)
   - Missing → create `.fluxloop/test-memory/` directory (first run)
3. Dual Write:
   - Server: `fluxloop data push` (upload key files)
   - Local: save to `.fluxloop/test-memory/agent-profile.md` with metadata
4. On completion: verify `agent-profile.md` is current (`git_commit` matches)

> 📎 Full protocol: read skills/_shared/CONTEXT_PROTOCOL.md
> 📎 Collection procedure: read skills/_shared/CONTEXT_COLLECTION.md

## Prerequisite

Run `fluxloop context show` first:
- ✅ Project selected → proceed
- ❌ No project → Prerequisite Resolution (📎 read skills/_shared/PREREQUISITE_RESOLUTION.md):
  - "Project setup is required. Would you like to run setup first?"
  - Approved: 📎 Run `skills/setup/SKILL.md` inline → on completion "✅ Setup complete. Continuing with context." → return to Step 1
  - Denied: stop

## Workflow

> ⚠️ Each Step must be executed sequentially. Do not batch Bash/Read calls in parallel. (📎 See CONTEXT_PROTOCOL.md)

### Step 1: Check Existing Profile

Check if `.fluxloop/test-memory/agent-profile.md` exists.

**If exists**:
- Show existing profile summary
- Display `scan_date` and `git_commit` from metadata
- Ask: "Would you like to update the profile? (Yes/No)"
  - No → keep existing profile, go to Next Steps
  - Yes → proceed to Step 2

**If missing**: "This is the first scan." → proceed to Step 2

### Step 2: Codebase Scan

> 📎 Follow the collection procedure: read skills/_shared/CONTEXT_COLLECTION.md

Scan targets:
1. README.md (or README.rst, README)
2. Agent main file (file containing system prompt, tool definitions)
3. package.json / pyproject.toml / requirements.txt (dependencies)
4. API spec files (OpenAPI, GraphQL schema, etc.)
5. Test files (to understand existing test structure)

Structure the scan results:
- Agent name, role
- LLM model / provider
- System prompt summary (3-5 lines)
- Tools / API list (table)
- Key features (bullet list)
- Dependencies (key packages / services)

### Step 3: Interactive — Additional Documents

Ask: "Do you have any reference documents? (enter path / skip)"
- Path entered → include that file in the scan
- skip → proceed

### Step 4: Intent Refine (Upload project context to server)

Refine and send the project intent to the FluxLoop server based on scan results from Step 2.

> 💡 **What is Intent Refine?** This step summarizes the agent's purpose, capabilities, and tech stack into a format the server can understand and transmit it. The server analyzes this information to improve scenario generation and test quality.

```bash
fluxloop intent refine --intent "<1-3 sentence summary of agent purpose and key capabilities>"
```

- `--intent` parameter: auto-generated based on Step 2 scan results (agent role, key features, tech stack)
- Server analyzes/refines the intent and stores it → used in subsequent scenario/test steps
- Success response: "✓ Intent extracted successfully"
- On failure: display error and proceed to Step 5 (intent upload failure does not block the workflow)

> 📎 Staging environment: read skills/_shared/STAGING.md (add `--staging` flag for staging environments)

### Step 5: Server Upload (Dual Write — Server)

Upload key files:

```bash
fluxloop data push README.md
fluxloop data push <agent-main-file> --bind
```

- `--bind` links the file to the current scenario (use only when a scenario is selected)
- If no scenario exists, upload to the project library without `--bind`

> **Required result output**: After upload, display: `✅ Data → N files uploaded to project library`
> (Data actions have no URL — see POST_ACTIONS.md)

### Step 6: Local Save (Dual Write — Local)

Ensure `.fluxloop/test-memory/` directory exists (create if missing).

Save to `.fluxloop/test-memory/agent-profile.md`:
- Metadata: `<!-- scan_date: {current ISO8601} | git_commit: {git rev-parse --short HEAD} -->`
- Format: follow the template from `test-memory-template/agent-profile.md`
- Replace all placeholders with actual scan results

### Step 7: Profile Summary Output

Show the generated profile to the user:
- Key info: agent name, role, LLM, number of tools, key features
- Ask: "Does this profile look correct? Let me know if anything needs to be changed."

## Error Handling

| Error | Response |
|-------|----------|
| Project not set up | Apply Prerequisite Resolution → suggest inline setup execution |
| No README found | Fall back to other files (pyproject.toml, main file); create a limited profile |
| `fluxloop intent refine` failure | Log error, proceed to next step (best-effort — does not block workflow) |
| `fluxloop data push` failure | Check network, verify file path, confirm login status |
| `git rev-parse` failure (not a git repo) | Set `git_commit` to `no-git`; note that stale detection is unavailable |

## Next Steps

Profile ready. Available next action:
- Scenario creation tailored to the agent profile (scenario skill)

## Quick Reference

| Step | Command |
|------|---------|
| Check | `fluxloop context show` |
| Intent | `fluxloop intent refine --intent "..."` |
| Upload | `fluxloop data push <file>` |
| Upload + bind | `fluxloop data push <file> --bind` |
| Git hash | `git rev-parse --short HEAD` |

> 📎 Full CLI reference: read skills/_shared/QUICK_REFERENCE.md

## Key Rules

1. Always check existing `agent-profile.md` before scanning — avoid redundant work
2. Use the template from `test-memory-template/agent-profile.md` for output format
3. Include metadata comment (`scan_date`, `git_commit`) — required for stale detection
4. Dual Write: server upload (`data push`) and local save (`agent-profile.md`) at the same time
5. Use `--bind` only when a scenario is already selected
6. Ask for additional reference documents in interactive mode
7. If no git repo, set `git_commit: no-git` and note stale detection is unavailable
8. Show profile summary and ask for confirmation before finalizing
9. On update: overwrite the entire `agent-profile.md` (not append)
