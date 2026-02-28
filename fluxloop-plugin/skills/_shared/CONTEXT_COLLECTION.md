# Context Collection Procedure

## Collection Steps

1. **Codebase scan**: README, agent main file, package.json/pyproject.toml, API specs, test files
2. **Generate summary**: Agent name, role, system prompt, tools/API list, key features, dependencies
3. **Interactive mode**: `"Do you have any reference documents? (enter path / skip)"`
4. **Server upload**:
   ```bash
   fluxloop data push README.md
   fluxloop data push docs/api-spec.md --bind  # --bind links to the current scenario
   ```
5. **Local save**: Save the collected results to `.fluxloop/test-memory/agent-profile.md` with metadata

## agent-profile.md Metadata Format

Include metadata as an HTML comment at the top of the file:

```
<!-- scan_date: 2024-01-15T10:30:00Z | git_commit: a1b2c3d -->
```

## Stale Detection Criteria

- If `git_commit` differs from the current `git rev-parse --short HEAD`, the profile is considered stale
- On stale detection — 사용자에게 **무엇이 왜 오래되었는지** 설명:
  > "마지막 프로필 스캔 이후 코드가 변경되었습니다 (프로필: {old_commit} → 현재: {new_commit}).
  > 프로필은 에이전트의 기능·도구·의존성 정보를 담고 있어, 코드가 바뀌면 테스트 정확도에 영향을 줄 수 있습니다.
  > 업데이트할까요? (Yes/No)"
  - Yes → run the collection procedure above
  - No → continue with the existing profile
