# Multi-turn Test Rules

## Core Rule

All multi-turn commands must start with the `!` prefix.

## Execution Commands

```bash
# OpenAI (default)
! fluxloop test --scenario <name> --multi-turn --max-turns 5

# Anthropic
! fluxloop test --scenario <name> --multi-turn --max-turns 5 --supervisor-provider anthropic
```

> ⚠️ Required: `!` prefix + `--multi-turn` flag
