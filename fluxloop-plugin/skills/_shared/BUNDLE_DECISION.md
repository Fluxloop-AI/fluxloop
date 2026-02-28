# Bundle Selection Decision Tree

Check existing data first, then offer choices to the user.

## Decision Flow

```
fluxloop bundles list --scenario-id <id> --format json
  │
  ├─ Multiple bundles → show list, "Which bundle would you like to use?"
  │   └─ Selected → sync pull (2 commands)
  │
  ├─ One bundle → "Use existing / Create new?"
  │   ├─ Use existing → sync pull (2 commands)
  │   └─ Create new → proceed to inputs check
  │
  └─ No bundle → fluxloop inputs list --scenario-id <id> --format json
      │
      ├─ Multiple input sets → show list, "Which input set would you like to use?"
      │   └─ Selected → bundle publish (3 commands)
      │
      ├─ One input set → "Use existing / Create new?"
      │   ├─ Use existing → bundle publish (3 commands)
      │   └─ Create new → full generation (5 commands)
      │
      └─ No input set → full generation (각 단계 완료 후 결과 출력 필수)
          1. fluxloop personas suggest --scenario-id <id>
             → ✅ Personas → N개 생성됨 + 이름 목록
          2. fluxloop inputs synthesize --scenario-id <id> --total-count N
             → ✅ Input Set → {id} (N개 입력) 🔗 URL + 내용 요약
             → 409 (`DATA_CONTEXT_NOT_READY`/`DATA_SUMMARY_MISSING`/`DATA_SUMMARY_STALE`) 발생 시 CLI 안내 문구를 따른 뒤 동일 명령 재시도
          3. fluxloop bundles publish --scenario-id <id> --input-set-id <id>
             → ✅ Bundle → v1 ({id}) 🔗 URL
```

## ID Extraction

list 명령 실행 시 반드시 `--format json`을 사용하여 ID를 추출한다. 테이블 출력은 터미널 폭에 따라 UUID가 잘릴 수 있으므로, JSON 출력에서 전체 ID를 안전하게 파싱한다.

## Display Format for Multiple Resources

When multiple bundles or input sets exist, show identifying information:

```
Agent: Found 3 existing bundles:
       1. v3 (stress-test, 20 inputs, 1 day ago)
       2. v2 (happy-path, 5 inputs, 3 days ago)
       3. v1 (edge-cases, 10 inputs, 7 days ago)

       Which bundle to use? Or create new?
```

Key info to display: **version/name, tag/description, count, created date**

## Simplified Flow for Comparison Tests

prompt-compare only needs a small number of inputs, so it follows a simplified flow:

```
bundles list --format json → exists → select
             → none → inputs list --format json → exists → select and publish
                                  → none → small-scale generation (각 단계 결과 출력 필수):
                                    1. fluxloop personas suggest --scenario-id <id>
                                       → ✅ Personas → N개 생성됨 + 이름 목록
                                    2. fluxloop inputs synthesize --scenario-id <id> --total-count 2
                                       → ✅ Input Set → {id} (N개 입력) 🔗 URL + 내용 요약
                                       → 409 (`DATA_CONTEXT_NOT_READY`/`DATA_SUMMARY_MISSING`/`DATA_SUMMARY_STALE`) 발생 시 CLI 안내 문구를 따른 뒤 동일 명령 재시도
                                    3. fluxloop bundles publish --scenario-id <id> --input-set-id <id>
                                       → ✅ Bundle → v1 ({id}) 🔗 URL
```
