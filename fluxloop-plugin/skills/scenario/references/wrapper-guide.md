# Agent Wrapper Guide

## When is Wrapper Needed?

| Agent Type | Wrapper? |
|------------|----------|
| Simple function `def run(input: str) -> str` | No — direct call |
| Class/stateful agent | Yes — wrap initialization |
| External dependencies (DB, broker, API) | Yes — wrap dependency injection |

## Setup Steps

1. Create: `.fluxloop/scenarios/<name>/agents/wrapper.py`
2. Update: `configs/simulation.yaml` → `runner.target: "agents.wrapper:run"`
3. Debug: `python -c "from agents.wrapper import run; print(run('test'))"`

## Python Wrapper Template

```python
# .fluxloop/scenarios/<name>/agents/wrapper.py
import uuid
from my_agent import AgentService

_agent = None

def run(input_text: str, metadata: dict = None) -> str:
    """FluxLoop test entry point. Must return string."""
    global _agent
    if _agent is None:
        _agent = AgentService()  # Initialize once

    conversation_id = str(uuid.uuid4())
    response = _agent.process(conversation_id, input_text)
    return str(response)
```

## YAML Config Template

```yaml
# configs/simulation.yaml
runner:
  target: "agents.wrapper:run"
```

## Async Version

```python
def run(input_text: str, metadata: dict = None) -> str:
    return asyncio.run(my_async_agent.process(input_text))
```
