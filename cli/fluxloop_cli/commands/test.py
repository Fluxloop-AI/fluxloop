"""
Test command to run pull -> run -> upload workflow.
"""

from __future__ import annotations

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
import yaml
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from ..config_loader import load_experiment_config, load_project_config
from ..constants import DEFAULT_CONFIG_PATH, FLUXLOOP_DIR_NAME, SCENARIOS_DIR_NAME, STATE_DIR_NAME
from ..project_paths import resolve_config_path
from ..runner import ExperimentRunner
from fluxloop.schemas import MultiTurnConfig
from ..turns import (
    TurnRecorder,
    format_warning_for_display,
    load_contracts,
    load_criteria_items,
    load_guardrails_from_config,
    render_result_markdown,
    write_latest_result_link,
)
from . import sync


app = typer.Typer(help="Run pull -> run -> upload test workflow.")
console = Console()


def _resolve_scenario_dir(scenario: Optional[str], base_dir: Optional[Path] = None) -> Path:
    """Resolve the scenario directory path."""
    if scenario:
        base = (base_dir or Path.cwd()).resolve()
        return base / FLUXLOOP_DIR_NAME / SCENARIOS_DIR_NAME / scenario
    return Path.cwd().resolve()


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    config_file: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config", "-c", help="Experiment configuration file"
    ),
    scenario: Optional[str] = typer.Option(None, "--scenario", help="Scenario name (folder in .fluxloop/scenarios/)"),
    skip_upload: Optional[bool] = typer.Option(
        None, "--skip-upload/--no-skip-upload", help="Skip sync upload step"
    ),
    stream_turns: Optional[bool] = typer.Option(
        None, "--stream-turns/--no-stream-turns", help="Stream turns to Web while running"
    ),
    stream_after_upload: Optional[bool] = typer.Option(
        None,
        "--stream-after-upload/--stream-during-run",
        help="Stream turns only after upload completes (default: during run)",
    ),
    multi_turn: Optional[bool] = typer.Option(
        None,
        "--multi-turn/--no-multi-turn",
        help="Enable or disable multi-turn supervisor loop",
    ),
    max_turns: Optional[int] = typer.Option(
        None,
        "--max-turns",
        help="Maximum number of turns per conversation (requires --multi-turn)",
    ),
    supervisor_provider: Optional[str] = typer.Option(
        None,
        "--supervisor-provider",
        help="Supervisor LLM provider (openai, anthropic)",
    ),
    supervisor_model: Optional[str] = typer.Option(
        None,
        "--supervisor-model",
        help="Supervisor LLM model (e.g., gpt-5-mini, claude-sonnet-4-5-20250514)",
    ),
    supervisor_api_key: Optional[str] = typer.Option(
        None,
        "--supervisor-api-key",
        help="API key for supervisor LLM (or use OPENAI_API_KEY/ANTHROPIC_API_KEY env)",
    ),
    smoke: bool = typer.Option(False, "--smoke", help="Run a smoke test"),
    full: bool = typer.Option(False, "--full", help="Run full test"),
    quiet: bool = typer.Option(False, "--quiet", help="Minimal output"),
    no_collector: bool = typer.Option(False, "--no-collector", help="Disable collector"),
):
    """
    Run FluxLoop test workflow (run -> upload).
    
    Runs from .fluxloop/scenarios/{scenario}/ if --scenario is specified,
    or the current directory otherwise.
    
    Inputs must be pulled first with 'fluxloop sync pull --bundle-version-id <id>'.
    """
    if ctx.invoked_subcommand is not None:
        return

    scenario_root = _resolve_scenario_dir(scenario)
    project_config, _ = load_project_config(
        config_file, scenario=scenario
    )
    test_config = project_config.get("test", {}) if isinstance(project_config, dict) else {}

    if smoke and full:
        raise typer.BadParameter("Choose only one of --smoke or --full.")

    mode = "smoke" if smoke else ("full" if full else test_config.get("default_mode", "full"))
    do_upload = (
        not skip_upload
        if skip_upload is not None
        else bool(test_config.get("auto_upload", True))
        )

    resolved_config = resolve_config_path(config_file, scenario)
    config = load_experiment_config(
        resolved_config,
        scenario=scenario,
        require_inputs_file=False,
    )

    # Apply multi-turn options
    if any(v is not None for v in [multi_turn, max_turns, supervisor_provider, supervisor_model, supervisor_api_key]):
        if config.multi_turn is None:
            config.multi_turn = MultiTurnConfig()
        if multi_turn is not None:
            config.multi_turn.enabled = multi_turn
        if max_turns is not None:
            config.multi_turn.max_turns = max_turns
        if supervisor_provider is not None:
            config.multi_turn.supervisor.provider = supervisor_provider
        if supervisor_model is not None:
            config.multi_turn.supervisor.model = supervisor_model
        if supervisor_api_key is not None:
            config.multi_turn.supervisor.api_key = supervisor_api_key

    # Check if inputs file exists
    if config.inputs_file:
        source_dir = config.get_source_dir()
        inputs_path = Path(config.inputs_file)
        resolved_inputs = (
            (source_dir / inputs_path).resolve() if source_dir and not inputs_path.is_absolute() else inputs_path
        )
        if not resolved_inputs.exists():
            console.print(f"[red]✗[/red] Inputs file not found: {resolved_inputs}")
            console.print()
            console.print("[bold]Run sync pull first:[/bold]")
            console.print("  [dim]fluxloop bundles list --scenario-id <scenario_id>[/dim]")
            console.print("  [dim]fluxloop sync pull --bundle-version-id <bundle_version_id>[/dim]")
            console.print()
            console.print("[dim]Then run test:[/dim]")
            console.print(f"  [dim]fluxloop test --scenario {scenario or '<name>'}[/dim]")
            raise typer.Exit(1)

    if mode == "smoke":
        config.iterations = 1
        inputs_path = Path(config.inputs_file) if config.inputs_file else None
        if inputs_path:
            source_dir = config.get_source_dir()
            resolved_inputs = (
                (source_dir / inputs_path).resolve() if source_dir and not inputs_path.is_absolute() else inputs_path
            )
            if resolved_inputs.exists():
                payload = yaml.safe_load(resolved_inputs.read_text()) or {}
                entries = payload.get("inputs") if isinstance(payload, dict) else payload
                if isinstance(entries, list) and entries:
                    smoke_payload = (
                        {"inputs": entries[:1]} if isinstance(payload, dict) else entries[:1]
                    )
                    state_dir = scenario_root / STATE_DIR_NAME
                    smoke_path = state_dir / "smoke_inputs.yaml"
                    smoke_path.parent.mkdir(parents=True, exist_ok=True)
                    smoke_path.write_text(
                        yaml.safe_dump(smoke_payload, sort_keys=False, allow_unicode=True),
                        encoding="utf-8",
                    )
                    config.inputs_file = str(smoke_path)

    guardrails = load_guardrails_from_config(project_config)
    runner = ExperimentRunner(config, no_collector=no_collector)
    recorder = TurnRecorder(runner.output_dir / "turns.jsonl", guardrails)
    state_dir = scenario_root / STATE_DIR_NAME
    criteria_items = load_criteria_items(state_dir / "criteria")
    contracts = load_contracts(state_dir / "contracts")
    result_path = runner.output_dir / "result.md"
    result_path.write_text("", encoding="utf-8")
    latest_path = write_latest_result_link(scenario_root, result_path)

    stream_enabled = stream_turns
    if stream_enabled is None:
        stream_enabled = bool(
            test_config.get("turn_streaming", True)
            or os.getenv("FLUXLOOP_TURN_STREAMING") in ("1", "true", "True")
        )
    if stream_after_upload is None:
        stream_after_upload = bool(
            test_config.get("turn_stream_after_upload", False)
            or os.getenv("FLUXLOOP_TURN_STREAM_AFTER_UPLOAD") in ("1", "true", "True")
        )
    stream_endpoint = test_config.get("turn_stream_endpoint") or os.getenv(
        "FLUXLOOP_TURN_STREAM_ENDPOINT"
    )
    stream_retries = int(test_config.get("turn_stream_retry_max", 3))
    stream_backoff = float(test_config.get("turn_stream_backoff_seconds", 1.0))
    stream_timeout = float(test_config.get("turn_stream_timeout_seconds", 10.0))
    stream_executor = ThreadPoolExecutor(max_workers=2) if stream_enabled else None
    stream_futures = []
    if stream_enabled:
        sync._load_env(scenario)

    run_id_map = {}
    if stream_enabled and not stream_after_upload:
        inputs = asyncio.run(runner._load_inputs())
        persona_map = {p.name: p for p in (config.personas or [])}
        use_entry_persona = config.has_external_inputs()

        inputs_path, _ = sync._resolve_inputs_path(scenario, None, config_file)
        input_map = sync._load_inputs_mapping(inputs_path)

        def _lookup_input_item_id(entry: dict, persona_name: Optional[str]) -> Optional[str]:
            metadata = entry.get("metadata") or {}
            direct = metadata.get("input_item_id")
            if direct:
                return direct
            input_text = entry.get("input")
            if not input_text:
                return None
            key = (input_text, persona_name)
            if key in input_map and input_map[key]:
                return input_map[key][0]
            key = (input_text, None)
            if key in input_map and input_map[key]:
                return input_map[key][0]
            fallback_keys = [k for k in input_map.keys() if k[0] == input_text and input_map[k]]
            if fallback_keys:
                fallback_keys.sort(key=lambda pair: "" if pair[1] is None else str(pair[1]))
                return input_map[fallback_keys[0]][0]
            return None

        runs_payload = []
        for iteration in range(config.iterations):
            if use_entry_persona:
                for entry in inputs:
                    persona = runner._resolve_entry_persona(entry, persona_map)
                    run_id = str(sync.uuid4())
                    key = (
                        iteration,
                        persona.name if persona else None,
                        entry.get("source_index"),
                        entry.get("input"),
                    )
                    run_id_map[key] = run_id
                    metadata = entry.get("metadata") or {}
                    input_item_id = _lookup_input_item_id(
                        entry, persona.name if persona else None
                    )
                    if not input_item_id:
                        raise typer.BadParameter(
                            "input_item_id not found. Run sync pull first."
                        )
                    runs_payload.append(
                        {
                            "id": run_id,
                            "input_item_id": input_item_id,
                            "persona_id": metadata.get("persona_id"),
                            "status": "running",
                        }
                    )
            else:
                personas = config.personas or [None]
                for persona in personas:
                    for entry in inputs:
                        run_id = str(sync.uuid4())
                        key = (
                            iteration,
                            persona.name if persona else None,
                            entry.get("source_index"),
                            entry.get("input"),
                        )
                        run_id_map[key] = run_id
                        metadata = entry.get("metadata") or {}
                        input_item_id = _lookup_input_item_id(
                            entry, persona.name if persona else None
                        )
                        if not input_item_id:
                            raise typer.BadParameter(
                                "input_item_id not found. Run sync pull first."
                            )
                        runs_payload.append(
                            {
                                "id": run_id,
                                "input_item_id": input_item_id,
                                "persona_id": metadata.get("persona_id"),
                                "status": "running",
                            }
                        )

        sync.precreate_runs(
            runs=runs_payload,
            experiment_id=runner.output_dir.name,
            scenario=scenario,
            base_dir=None,
            api_url=None,
            api_key=None,
            bundle_version_id=None,
            quiet=quiet,
        )

    if not quiet:
        console.print(f"[Run] Executing {mode} test...")

    def _turn_record_callback(turn_payload: dict) -> None:
        record = recorder.record_turn(turn_payload)
        if stream_enabled and stream_executor and not stream_after_upload:
            stream_futures.append(
                stream_executor.submit(
                    sync.stream_turn,
                    turn=record,
                    experiment_id=runner.output_dir.name,
                    scenario_root=scenario_root,
                    api_url=None,
                    api_key=None,
                    endpoint=stream_endpoint,
                    max_retries=stream_retries,
                    backoff_seconds=stream_backoff,
                    timeout_seconds=stream_timeout,
                    quiet=quiet,
                )
            )
        if quiet:
            return
        if record.get("role") != "assistant":
            return
        run_id = record["run_id"]
        turn_index = recorder.get_assistant_turn_count(run_id)
        total_runs = len(run_id_map) if run_id_map else None
        run_number = None
        if run_id_map:
            try:
                run_number = list(run_id_map.values()).index(run_id) + 1
            except ValueError:
                run_number = None
        duration_ms = record.get("duration_ms")
        duration = f"{duration_ms / 1000:.1f}s" if duration_ms is not None else "-"
        warning_msg = format_warning_for_display(record.get("warnings", []))
        if warning_msg:
            status = f"⚠️ - {warning_msg}"
        else:
            status = "✓"
        if run_number is not None and total_runs is not None:
            console.print(
                f"  Run {run_number}/{total_runs} · Turn {turn_index}: {status} assistant ({duration})"
            )
        else:
            console.print(f"  Turn {turn_index}: {status} assistant ({duration})")
        content = record.get("content") or ""
        if content:
            console.print("    Response:")
            for line in str(content).splitlines():
                console.print(f"      {line}")
        # Update result.md progressively so latest_result is always available.
        result_path.write_text(
            render_result_markdown(list(recorder.iter_turns()), recorder.get_overall_summary(), criteria_items),
            encoding="utf-8",
        )

    try:
        asyncio.run(
            runner.run_experiment(
                turn_record_callback=_turn_record_callback,
                run_id_provider=(
                    None
                    if not run_id_map
                    else lambda variation, persona, iteration: run_id_map.get(
                        (
                            iteration,
                            persona.name if persona else None,
                            variation.get("source_index"),
                            variation.get("input"),
                        )
                    )
                ),
            )
        )
    except KeyboardInterrupt:
        console.print("[yellow]Test interrupted by user[/yellow]")
        raise typer.Exit(1)
    except Exception as exc:
        console.print(f"[red]Test failed:[/red] {exc}")
        raise typer.Exit(1)

    summary = recorder.get_overall_summary()
    turns = list(recorder.iter_turns())
    result_path.write_text(
        render_result_markdown(turns, summary, criteria_items), encoding="utf-8"
    )

    if do_upload:
        if not quiet:
            console.print("[Upload] Uploading to Web...")
        sync.upload(
            experiment_dir=runner.output_dir,
            config_file=config_file,
            scenario=scenario,
            api_url=None,
            api_key=None,
            bundle_version_id=None,
            quiet=quiet,
        )

    if stream_enabled and stream_after_upload:
        if not do_upload:
            if not quiet:
                console.print(
                    "[yellow]Turn streaming skipped: upload is disabled.[/yellow]"
                )
        else:
            if not quiet:
                console.print("[Stream] Streaming turns after upload...")
            for turn in turns:
                if stream_executor:
                    stream_futures.append(
                        stream_executor.submit(
                            sync.stream_turn,
                            turn=turn,
                            experiment_id=runner.output_dir.name,
                            scenario_root=scenario_root,
                            api_url=None,
                            api_key=None,
                            endpoint=stream_endpoint,
                            max_retries=stream_retries,
                            backoff_seconds=stream_backoff,
                            timeout_seconds=stream_timeout,
                            quiet=quiet,
                        )
                    )
                else:
                    sync.stream_turn(
                        turn=turn,
                        experiment_id=runner.output_dir.name,
                        scenario_root=scenario_root,
                        api_url=None,
                        api_key=None,
                        endpoint=stream_endpoint,
                        max_retries=stream_retries,
                        backoff_seconds=stream_backoff,
                        timeout_seconds=stream_timeout,
                        quiet=quiet,
                    )

    if stream_executor:
        for future in stream_futures:
            try:
                future.result()
            except Exception:
                continue
        stream_executor.shutdown(wait=True)

    if quiet:
        console.print(
            f"result: {summary.total_turns} turns, {summary.warning_count} warnings"
        )
        return

    console.print("═══════════════════════════════════════════")
    console.print(
        f"결과: {summary.total_turns} turns, {summary.warning_turns} warning turns"
    )
    if contracts:
        console.print("\n[Contracts]")
        for contract in contracts:
            ctype = contract.get("type", "contract")
            desc = contract.get("description", "")
            category = contract.get("category", "")
            prefix = {"must": "✓", "should": "○", "must_not": "✗"}.get(ctype, "•")
            cat_label = f" [{category}]" if category else ""
            console.print(f"  {prefix} {ctype.upper()}{cat_label}: {desc}")
    if criteria_items:
        console.print("\n[평가 기준]")
        for item in criteria_items:
            console.print(f"- {item}")
    console.print("───────────────────────────────────────────")
    console.print(f"📁 상세 확인: fluxloop test results --scenario {scenario or '<name>'}")
    console.print("═══════════════════════════════════════════")


@app.command("results")
def results(
    scenario: Optional[str] = typer.Option(
        None, "--scenario", help="Scenario name (folder in .fluxloop/scenarios/)"
    ),
    raw: bool = typer.Option(
        False, "--raw", help="Output raw markdown without formatting"
    ),
):
    """
    Show latest test results.
    
    Displays the contents of .fluxloop/scenarios/{scenario}/.state/latest_result.md
    """
    scenario_root = _resolve_scenario_dir(scenario)
    result_path = scenario_root / STATE_DIR_NAME / "latest_result.md"
    
    if not result_path.exists():
        console.print("[yellow]No test results found.[/yellow]")
        console.print(f"[dim]Run test first: fluxloop test --scenario {scenario or '<name>'}[/dim]")
        raise typer.Exit(1)
    
    content = result_path.read_text(encoding="utf-8")
    
    if raw:
        print(content)
    else:
        from rich.markdown import Markdown
        console.print(Markdown(content))
