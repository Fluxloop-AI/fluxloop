# test-memory/

A directory that stores working context across agent test sessions.
Skills automatically create and manage these files. You may also edit them manually if needed.

> This directory is created at `.fluxloop/test-memory/` inside the user's project.
> No manual copying required — skills create it automatically on first run.

## Files

| File | Purpose | Created by | Referenced by |
|------|---------|------------|---------------|
| `agent-profile.md` | Profile of the agent under test | context | scenario, test, evaluate, prompt-compare |
| `test-strategy.md` | Test strategy and scenario configuration | scenario | test, evaluate |
| `prompt-versions.md` | Prompt version history | prompt-compare | evaluate |
| `results-log.md` | Cumulative test results log | test, evaluate | all |
| `learnings.md` | Insights and improvement notes | evaluate | scenario, prompt-compare |

## Notes

- `.fluxloop/test-memory/` is inside `.fluxloop/` — if `.fluxloop/` is in `.gitignore`, it is excluded automatically
- Skills proceed without error even if files don't exist (treated as first run)
- If the `git_commit` metadata in `agent-profile.md` differs from the current commit, the profile is considered stale
