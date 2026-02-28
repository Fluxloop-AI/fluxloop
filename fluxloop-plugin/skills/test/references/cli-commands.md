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
- **Verify full UUID** — CLI 테이블은 터미널 폭에 따라 UUID를 잘라서 표시함. 추출한 ID가 36자(`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)인지 반드시 확인. 36자 미만이면 잘린 것이므로 list 명령을 재실행하여 전체 ID를 확보한 후 사용할 것.
