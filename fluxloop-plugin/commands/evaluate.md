---
description: Trigger server-side evaluation for an experiment
allowed-tools: [Bash]
---

# FluxLoop Evaluate

Trigger server-side evaluation for an experiment (insights + recommendations).

## Run

```bash
fluxloop evaluate --experiment-id <experiment_id>
```

## Wait for completion (recommended)

```bash
fluxloop evaluate --experiment-id <experiment_id> --wait
```

## Options

| Option | Description |
|--------|-------------|
| `--wait` | Poll until the evaluation finishes |
| `--timeout <sec>` | Max seconds to wait (default 600) |
| `--poll-interval <sec>` | Polling interval in seconds |
| `--force-rerun` | Force evaluation even if a job exists |

## Notes

- When status is `completed` or `partial` (with at least one run completed), insights and recommendations are generated and the CLI prints the latest headlines.
- If the job stays `queued` for >30s without a worker lock (`locked_at`), the CLI warns that workers may be down or backlog is high.
