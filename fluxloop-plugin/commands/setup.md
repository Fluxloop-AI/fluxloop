---
description: First-time setup guide for FluxLoop CLI
allowed-tools: [Bash]
---

# FluxLoop Setup

First-time installation and configuration guide.

## Prerequisites

- Python 3.11+
- uv (recommended) or pip

## Installation

> Important: install `fluxloop-cli` for command-line usage. Do not install `fluxloop` unless you explicitly need the SDK.

### Option 1: uv (Recommended)

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install FluxLoop CLI
uv tool install fluxloop-cli
```

### Option 2: pip

```bash
pip install fluxloop-cli
```

### Option 3: Development Install (from source)

```bash
git clone https://github.com/fluxloop/fluxloop.git
cd fluxloop/packages/cli
uv pip install -e .
```

## Verify Installation

```bash
fluxloop --version
fluxloop doctor
```

## First-Time Login

```bash
# Login (opens browser for authentication)
fluxloop auth login

# Check login status
fluxloop auth status
```

## Initial Configuration

```bash
# 1. List available projects
fluxloop projects list

# 2. Select a project (or create new)
fluxloop projects select <project_id>
# OR
fluxloop projects create --name "my-agent"

# 3. Initialize local scenario (from workspace root)
fluxloop init scenario my-test
cd .fluxloop/scenarios/my-test

# 4. Create API key for sync operations
fluxloop apikeys create
```

## Quick Test

```bash
# Run a smoke test to verify everything works
fluxloop test --scenario my-test --smoke --skip-upload
```

## Troubleshooting

### "Command not found: fluxloop"

```bash
# Check if installed
which fluxloop

# If using uv tool, ensure PATH includes ~/.local/bin
export PATH="$HOME/.local/bin:$PATH"
```

### "Authentication required"

```bash
fluxloop auth login
```

### "No project selected"

```bash
fluxloop projects list
fluxloop projects select <id>
```

### "No solution found when resolving dependencies for fluxloop"

You likely tried to install `fluxloop` (SDK) instead of `fluxloop-cli` (CLI), or your Python version does not match the package requirement.

```bash
python --version
uv pip index versions fluxloop-cli
uv add fluxloop-cli
```

## Next Steps

After setup, use the agent-test skill for full workflow:
- "test my agent" - Full test cycle
- "generate test data" - Input synthesis
- "run simulation" - Execute tests
