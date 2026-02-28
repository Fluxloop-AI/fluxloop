# Test CLI Commands Reference

## API Key File

- Location: `.fluxloop/.env` (shared by all scenarios)
- Created by: `fluxloop apikeys create`
- Manual additions:
  - OpenAI: `OPENAI_API_KEY=sk-xxx`
  - Anthropic: `ANTHROPIC_API_KEY=sk-ant-xxx`

## Test Command Options

| Option | Description |
|--------|-------------|
| `--scenario <name>` | Specify scenario to test |
| `--multi-turn` | Enable multi-turn mode (requires `!` prefix) |
| `--max-turns <N>` | Maximum turns in multi-turn (default: 8) |
| `--supervisor-provider <provider>` | Use `anthropic` for Anthropic supervisor |

## Result Commands

| Command | Description |
|---------|-------------|
| `fluxloop test results --scenario <name>` | Formatted output |
| `fluxloop test results --scenario <name> --raw` | Raw markdown |

## Important Notes

- **Never use `--pull`** — always run `fluxloop sync pull` and `fluxloop test` as separate commands
- **Use `--format json` on list commands** — 테이블 출력은 터미널 폭에 따라 UUID가 잘릴 수 있으므로, ID 추출이 필요한 list 명령은 반드시 `--format json`을 사용한다.
- **Handle input readiness conflicts** — `inputs synthesize/refine` can return 409 (`DATA_CONTEXT_NOT_READY`, `DATA_SUMMARY_MISSING`, `DATA_SUMMARY_STALE`) right after `data push`; follow CLI guidance and retry after readiness.
