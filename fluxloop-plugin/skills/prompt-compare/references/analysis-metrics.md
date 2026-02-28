# Analysis Metrics Reference

## trace_summary.jsonl Structure

Each line in `trace_summary.jsonl` is a JSON object:

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

| Field | Description |
|-------|-------------|
| `trace_id` | Unique identifier for this trace |
| `iteration` | Run number (0-indexed) |
| `persona` | Persona used for this input |
| `input` | User input text |
| `output` | Agent response text |
| `duration_ms` | Response time in milliseconds |
| `success` | Whether the run completed successfully |
| `token_usage` | Input/output token counts |

## Per-Input Analysis Format

Group traces by `input` field, then compare across versions:

```markdown
## Input: "{input text}"

### {version_A_label} ({N} runs)
| # | Output Summary | Tokens | Time (ms) |
|---|----------|------|----------|
| 1 | [One-line key summary] | 150 | 1200 |
| 2 | ... | ... | ... |

### {version_B_label} ({N} runs)
| # | Output Summary | Tokens | Time (ms) |
|---|----------|------|----------|
| 1 | [One-line key summary] | 130 | 1100 |
| 2 | ... | ... | ... |

### Comparison
- **Within-version consistency**: {version_A} high / {version_B} medium
- **Cross-version difference**: clear / minimal
- **Key change**: [What changed concretely]
```

## Overall Summary Table Format

```markdown
## Overall Comparison

| Metric | {version_A} | {version_B} |
|------|----|----|
| Avg token count | 150 | 130 |
| Avg response time (ms) | 1200 | 1100 |
| Success rate | 100% | 100% |
| Within-version consistency | High | Medium |

### Conclusion
[Summarize impact by linking prompt changes to result changes]
```
