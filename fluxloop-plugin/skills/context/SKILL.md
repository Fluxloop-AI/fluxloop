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

Ask: "Do you have any reference documents or validation data? (enter path / skip)"
- Path entered → classify using the decision tree in Step 5-1:
  - Classified as GT but no scenario exists → "This file looks like validation data (Ground Truth). It will be uploaded as context for now. After creating a scenario, you can bind it as Ground Truth."
  - Classified as GT with active scenario → include for GT upload in Step 5-3
  - Classified as context → include for context upload in Step 5-2
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

Upload key files to the server. Before uploading, classify each file as **Context** or **Ground Truth**.

#### 5-1. Data Classification Decision Tree

> 💡 **Context vs Ground Truth**: Context data is reference material that informs test generation (e.g., docs, specs, code). Ground Truth (GT) data is a structured dataset with expected answers used to validate agent output correctness after test execution.

For each file the user provides, apply this decision tree **in order**:

```
File to upload
  │
  ├─ 1. User explicitly says "ground truth", "validation data",
  │     "expected answers", "정답 데이터", "검증 데이터"
  │     → ✅ Ground Truth
  │
  ├─ 2. User explicitly says "context", "reference", "참고 자료"
  │     → ✅ Context
  │
  ├─ 3. Check file type:
  │     │
  │     ├─ Unstructured (md, txt, pdf, docx, html)
  │     │   → ✅ Context (always)
  │     │
  │     └─ Structured (csv, json, jsonl, xlsx, tsv)
  │         │
  │         ├─ 4. Inspect content — does it have input+output column pairs?
  │         │     (e.g., "question"+"answer", "input"+"expected_output",
  │         │      "query"+"label", "prompt"+"response")
  │         │     → YES → ✅ Ground Truth (confirm with user)
  │         │     → NO or UNCLEAR → Step 5
  │         │
  │         └─ 5. Ask the user:
  │               "이 파일은 구조화된 데이터입니다. 용도를 선택해주세요:
  │                1) 참고 자료 (Context) — 테스트 입력 생성 시 참고용
  │                2) 정답 데이터 (Ground Truth) — 에이전트 출력의 정확성 검증용"
```

**Key distinction**:

| Aspect | Context | Ground Truth |
|--------|---------|--------------|
| Purpose | Informs input synthesis quality | Validates output correctness |
| File types | Any (md, pdf, csv, etc.) | Structured only (csv, json, jsonl, xlsx) |
| Requires scenario | No (project library) | Yes (must bind to scenario) |
| Has labels/answers | No | Yes (input + expected output columns) |
| Examples | README, API spec, user guide | Q&A pairs, expected outputs, scoring rubrics |

#### 5-2. Context Upload (default)

```bash
fluxloop data push README.md
fluxloop data push <agent-main-file> --bind
```

- `--bind` links the file to the current scenario (use only when a scenario is selected)
- If no scenario exists, upload to the project library without `--bind`

#### 5-3. Ground Truth Upload

> ⚠️ GT upload requires an active scenario. If no scenario exists, skip GT upload and note: "Ground Truth data can be added after scenario creation (scenario skill)."

```bash
fluxloop data push <gt-file> --usage ground-truth --scenario <scenario_id> \
  --label-column <col>
```

**GT options**:

| Option | Required | Description |
|--------|----------|-------------|
| `--usage ground-truth` | Yes | Activates GT mode (forces DATASET category) |
| `--scenario <id>` or `--bind` | Yes | GT must be bound to a scenario |
| `--label-column <col>` | Recommended | Column containing expected answers |
| `--split <train\|dev\|test>` | No | Data split |
| `--row-filter <expr>` | No | Filter rows before materialization |
| `--sampling-seed <N>` | No | Reproducibility seed (default: 42) |

After successful GT upload, the CLI auto-materializes GT profile and contracts. Display:

```
✅ Data (GT) → {filename} uploaded + bound to scenario
  data_id: {data_id}
  profile_id: {profile_id}
  gt_contract_count: {N}
```

> **Required result output**: After all uploads, display: `✅ Data → N files uploaded to project library` (+ GT summary if applicable)
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
| GT upload without scenario | "Ground Truth requires a scenario. Create one first (scenario skill), then upload with `--usage ground-truth`." |
| GT materialization 409 | "Check status: `fluxloop data gt status --scenario <id>`. Retry bind after processing completes." |
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
| Upload (GT) | `fluxloop data push <file> --usage ground-truth --scenario <id> --label-column <col>` |
| GT status | `fluxloop data gt status --scenario <id>` |
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
10. Classify files before upload: unstructured → context; structured with input+output pairs → likely GT (confirm with user)
11. GT upload requires an active scenario — defer GT files if no scenario exists
