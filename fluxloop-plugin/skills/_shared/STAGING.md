# Staging Environment

- When "staging" is mentioned, prefer the `--staging` flag (`--api-url` only for custom domains)
- Auth: `fluxloop auth login --staging --no-wait && fluxloop auth login --resume`
- Project: `fluxloop projects select <id> --staging` / `fluxloop projects create --name "my-agent" --language <code> --staging`
