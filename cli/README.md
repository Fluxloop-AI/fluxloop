# FluxLoop CLI

Command-line interface for running agent simulations.

## Installation

```
pip install fluxloop-cli
```

## Configuration Overview (v0.2.0)

FluxLoop CLI now stores experiment settings in four files under `configs/`:

- `configs/project.yaml` – project metadata, collector defaults
- `configs/input.yaml` – personas, base inputs, input generation options
- `configs/simulation.yaml` – runtime parameters (iterations, runner, replay args)
- `configs/evaluation.yaml` – evaluator definitions (rule-based, LLM judge, etc.)

The legacy `setting.yaml` is still supported, but new projects created with
`fluxloop init project` will generate the structured layout above.

## Key Commands

- `fluxloop init project` – scaffold a new project (configs, `.env`, examples)
- `fluxloop generate inputs` – produce input variations for the active project
- `fluxloop run experiment` – execute an experiment using `configs/simulation.yaml`
- `fluxloop parse experiment` – convert experiment outputs into readable artifacts and emit structured per-trace JSON at `per_trace_analysis/per_trace.jsonl`
- `fluxloop evaluate experiment` – run the LLM-driven evaluation pipeline (LLM-PT → rule aggregation → LLM-OV → HTML render). Requires the parsed per-trace file (or `--per-trace`) and writes an interactive report to `evaluation_report/report.html` by default.
- `fluxloop config set-llm` – update LLM provider/model in `configs/input.yaml`
- `fluxloop data push|bind|gt status` – upload/bind Ground Truth (Validation) datasets and inspect GT materialization status
- `fluxloop record enable|disable|status` – toggle recording mode across `.env` and simulation config
- `fluxloop doctor` – summarize Python, FluxLoop CLI/MCP, and MCP index state for the active environment
- `--yes/-y` (for `fluxloop run experiment`) – skip the interactive confirmation prompt, ideal for CI and the Pytest bridge

### Ground Truth Data Workflow

Use these commands to upload validation datasets, bind them to a scenario, and
check GT materialization progress:

```bash
# 1) Upload as Ground Truth + bind + materialize
fluxloop data push qa_pairs.csv \
  --usage ground-truth \
  --scenario <scenario_id> \
  --split test \
  --label-column expected_answer \
  --row-filter "lang = 'ko'"

# 2) Bind existing data as validation (auto materialize by default)
fluxloop data bind <data_id> \
  --scenario <scenario_id> \
  --role validation \
  --split test \
  --label-column expected_answer

# 3) Check GT materialization status
fluxloop data gt status --scenario <scenario_id> [--data-id <data_id>]
```

### Multi-turn supervisor options

`fluxloop run experiment` supports multi-turn orchestration out of the box:

- Toggle with `--multi-turn/--no-multi-turn`
- Limit depth via `--max-turns`
- Control tool approvals with `--auto-approve-tools/--manual-approve-tools`
- Override the supervisor persona target: `--persona-override`
- Point at a specific LLM: `--supervisor-provider`, `--supervisor-model`, `--supervisor-temperature`, `--supervisor-api-key`

These flags override the values in `configs/simulation.yaml` (`multi_turn` block). When enabled, the runner consults the supervisor after every turn to decide whether to continue and to synthesize the next realistic user message.

**Scripted Playback Mode**: For deterministic multi-turn scenarios, switch `supervisor.provider` to `mock` and populate `supervisor.metadata.scripted_questions` with a list of user messages. FluxLoop will replay them sequentially and terminate when the script ends—ideal for regression testing and demos.

Run `fluxloop --help` or `fluxloop <command> --help` for more detail.

## Pytest Bridge (0.2.29+)

- `fluxloop init pytest-template [project_root]` creates `tests/test_fluxloop_smoke.py`, already wired to the new `fluxloop_runner` fixture.
- Fixtures live in `fluxloop_cli.testing.pytest_plugin` and return a `FluxLoopTestResult`, so you can assert on `total_runs`, `success_rate`, or call `require_success()`.
- Full guide + CI example: see `docs/guides/pytest_bridge.md` (includes GitHub Actions workflow at `examples/ci/fluxloop_pytest.yml`).
- Typical workflow:
  1. `pip install -e packages/cli[dev]`
  2. `fluxloop init pytest-template .`
  3. `pytest -k fluxloop_smoke --maxfail=1`

## Evaluation Workflow

Evaluation now follows a two-step process so that multi-turn context is preserved:

1. `fluxloop run experiment` – produce `trace_summary.jsonl` (and optionally `observations.jsonl`).
2. `fluxloop parse experiment <experiment_dir>` – generate markdown summaries and a structured artifact at `per_trace_analysis/per_trace.jsonl`.
3. `fluxloop evaluate experiment <experiment_dir>` – consume that structured file, run LLM-based per-trace + overall analysis, and emit an interactive dashboard at `<experiment_dir>/evaluation_report/report.html` (override with `--output`).

`fluxloop evaluate` exits early with guidance when the per-trace artifact is missing. If you relocate the file, supply an explicit path with `--per-trace /path/to/per_trace.jsonl`.

## Quick Setup Script

To prepare a fresh checkout (create `.venv`, install dependencies, and run diagnostics):

```
bash scripts/setup_fluxloop_env.sh --target-source-root path/to/your/source
```

Options:

- `--python PATH` – choose a specific interpreter (default `python3`)
- `--target-source-root PATH` – pre-populate VSCode `fluxloop.targetSourceRoot`
- `--skip-doctor` – skip the final `fluxloop doctor` check

After running the script, open the folder in VSCode and use `FluxLoop: Show Environment Info`
or `FluxLoop: Run Doctor` to confirm the environment.

## Full Reset Script

To remove FluxLoop-only local state (`.fluxloop*`, `~/.fluxloop`), remove
project deps via `uv remove` (when `pyproject.toml` exists), and uninstall
`fluxloop-cli` / `fluxloop` from pipx/uv tool/pip:

```bash
bash reset_fluxloop_env.sh --dry-run
bash reset_fluxloop_env.sh --yes
```

Options:

- `--project-root PATH` - clean a specific project root

## Runner Integration Patterns

Configure how FluxLoop calls your code in `configs/simulation.yaml`:

- Module + function: `module_path`/`function_name` or `target: "module:function"`
- Class.method (zero-arg ctor): `target: "module:Class.method"`
- Module-scoped instance method: `target: "module:instance.method"`
- Class.method with factory: add `factory: "module:make_instance"` (+ `factory_kwargs`)
- Async generators: set `runner.stream_output_path` if your streamed event shape differs (default `message.delta`).

See full examples: `packages/website/docs-cli/configuration/runner-targets.md`.

## Developing

Install dependencies and run tests:

```
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
pytest
```

To package the CLI:

```
./build.sh
```
