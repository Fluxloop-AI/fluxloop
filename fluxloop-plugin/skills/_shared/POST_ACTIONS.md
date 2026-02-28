# Post-Action Summary

After every CLI action, output the result in this format:

`✅ [Action] → [summary] 🔗 https://...`

## URL Patterns

| Action | URL Pattern |
|--------|-------------|
| Data | (no URL) |
| Project | `https://alpha.app.fluxloop.ai/simulate/scenarios?project={project_id}` |
| Scenario | `https://alpha.app.fluxloop.ai/simulate/scenarios/{scenario_id}?project={project_id}` |
| Contracts | same as Scenario URL |
| Input Set | `https://alpha.app.fluxloop.ai/simulate/scenarios/{scenario_id}/inputs/{input_set_id}?project={project_id}` |
| Bundle | `https://alpha.app.fluxloop.ai/simulate/scenarios/{scenario_id}/bundles/{bundle_version_id}?project={project_id}` |
| Experiment | `https://alpha.app.fluxloop.ai/release/experiments/{experiment_id}/evaluation?project={project_id}` |

## Examples

| Phase | Example |
|-------|---------|
| Login | `✅ Login → user@example.com` |
| Project | `✅ Project → "my-bot" (proj_123) 🔗 https://alpha.app.fluxloop.ai/simulate/scenarios?project=proj_123` |
| Data | `✅ Data → 3 files uploaded to project library` |
| Scenario | `✅ Scenario → "Happy Path" (scn_456) 🔗 https://alpha.app.fluxloop.ai/simulate/scenarios/scn_456?project=proj_123` |
| Contracts | `✅ Contracts → 3 generated 🔗 https://alpha.app.fluxloop.ai/simulate/scenarios/scn_456?project=proj_123` |
| Input Set | `✅ Input Set → inp_789 (10 inputs) 🔗 https://alpha.app.fluxloop.ai/simulate/scenarios/scn_456/inputs/inp_789?project=proj_123` |
| QC | `✅ QC → format: 10/10, duplicates: 0, diversity: high` |
| Bundle | `✅ Bundle → v1 (bnd_012) 🔗 https://alpha.app.fluxloop.ai/simulate/scenarios/scn_456/bundles/bnd_012?project=proj_123` |
| Test | `✅ Test → exp_abc (10 runs) 🔗 https://alpha.app.fluxloop.ai/release/experiments/exp_abc/evaluation?project=proj_123` |
| Eval | `✅ Evaluation → 3 insights 🔗 https://alpha.app.fluxloop.ai/release/experiments/exp_abc/evaluation?project=proj_123` |
