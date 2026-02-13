"""
Sync command for pulling bundles and uploading results.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from uuid import UUID, uuid4

import httpx
import typer
import yaml
from rich.console import Console

from ..api_utils import resolve_api_url
from ..config_loader import load_experiment_config
from ..constants import DEFAULT_CONFIG_PATH, FLUXLOOP_DIR_NAME, SCENARIOS_DIR_NAME
from ..context_manager import get_current_web_project_id, get_current_scenario
from ..environment import load_env_chain
from ..project_paths import resolve_config_path
from ..turns import load_turns, summarize_turns, utc_now_iso


app = typer.Typer(help="Sync bundle inputs and upload run results.")
console = Console()


def _get_effective_scenario(scenario: Optional[str]) -> Optional[str]:
    """Get scenario from argument or current context.
    
    Priority:
    1. Explicit --scenario argument
    2. Current scenario from context (context.json)
    """
    if scenario:
        return scenario
    ctx_scenario = get_current_scenario()
    if ctx_scenario and ctx_scenario.local_path:
        # "scenarios/dataset-diagnosis" → "dataset-diagnosis"
        return Path(ctx_scenario.local_path).name
    return None


def _resolve_scenario_dir(scenario: Optional[str], base_dir: Optional[Path] = None) -> Path:
    """Resolve the scenario directory path.
    
    v2 structure: .fluxloop/scenarios/{scenario}/
    """
    if scenario:
        base = (base_dir or Path.cwd()).resolve()
        return base / FLUXLOOP_DIR_NAME / SCENARIOS_DIR_NAME / scenario
    return Path.cwd().resolve()


def _resolve_relative_path(path: Path, scenario: Optional[str], base_dir: Optional[Path] = None) -> Path:
    """Resolve a path relative to the scenario directory."""
    if path.is_absolute():
        return path.expanduser().resolve()
    scenario_dir = _resolve_scenario_dir(scenario, base_dir)
    return (scenario_dir / path).resolve()


def _get_workspace_env_path(base_dir: Optional[Path] = None) -> Optional[Path]:
    """Get the workspace-level .env path (.fluxloop/.env)."""
    from ..project_paths import find_workspace_root
    workspace_root = find_workspace_root(base_dir)
    if workspace_root:
        return workspace_root / FLUXLOOP_DIR_NAME / ".env"
    # Fall back to current directory
    cwd_fluxloop = (base_dir or Path.cwd()) / FLUXLOOP_DIR_NAME
    if cwd_fluxloop.exists():
        return cwd_fluxloop / ".env"
    return None


def _load_env(scenario: Optional[str], base_dir: Optional[Path] = None) -> None:
    """Load environment variables from .env files.
    
    Load order (later overrides earlier):
    1. .fluxloop/.env (workspace level, shared by all scenarios - API keys)
    2. .fluxloop/scenarios/{scenario}/.env (scenario specific - OPENAI_API_KEY, etc.)
    """
    import fluxloop
    
    # Load workspace-level .env first (API keys shared by all scenarios)
    workspace_env = _get_workspace_env_path(base_dir)
    if workspace_env and workspace_env.exists():
        try:
            fluxloop.load_env(workspace_env, override=True, refresh_config=False)
        except Exception:
            pass  # Ignore errors, scenario env will be tried next
    
    # Then load scenario-level .env (can override workspace settings)
    source_dir = _resolve_scenario_dir(scenario, base_dir)
    load_env_chain(source_dir, refresh_config=False)


def _resolve_api_key(override: Optional[str]) -> str:
    key = (
        override
        or os.getenv("FLUXLOOP_SYNC_API_KEY")
        or os.getenv("FLUXLOOP_API_KEY")
    )
    if not key:
        raise typer.BadParameter(
            "Sync API key is not set. "
            "Use 'fluxloop apikeys create' to generate one, "
            "or 'fluxloop config set-sync-key' if you have an existing key."
        )
    return key


def _post_with_retry(
    client: httpx.Client,
    endpoint: str,
    *,
    payload: Dict[str, Any],
    headers: Dict[str, str],
    max_retries: int = 3,
    backoff_seconds: float = 1.0,
    timeout_seconds: float = 10.0,
) -> httpx.Response:
    attempt = 0
    while True:
        try:
            resp = client.post(
                endpoint,
                json=payload,
                headers=headers,
                timeout=timeout_seconds,
            )
            resp.raise_for_status()
            return resp
        except Exception:
            attempt += 1
            if attempt > max_retries:
                raise
            time.sleep(backoff_seconds * (2 ** (attempt - 1)))


def precreate_runs(
    *,
    runs: List[Dict[str, Any]],
    experiment_id: str,
    scenario: Optional[str] = None,
    base_dir: Optional[Path] = None,
    api_url: Optional[str] = None,
    api_key: Optional[str] = None,
    bundle_version_id: Optional[str] = None,
    quiet: bool = False,
) -> Optional[str]:
    """
    Precreate runs via /api/sync/upload so streaming can reference existing runs.
    """
    _load_env(scenario, base_dir)
    api_url = resolve_api_url(api_url)
    api_key = _resolve_api_key(api_key)
    scenario_root = _resolve_scenario_dir(scenario, base_dir)
    sync_state = _read_sync_state(scenario_root)

    bundle_version_id = bundle_version_id or sync_state.get("bundle_version_id")
    if not bundle_version_id:
        raise typer.BadParameter("bundle_version_id is required. Run sync pull first.")

    payload = {
        "bundle_version_id": bundle_version_id,
        "run_batch": {
            "experiment_id": experiment_id,
            "execution_target": "local",
            "status": "running",
        },
        "runs": runs,
        "run_results": [],
        "artifacts": [],
        "upload_meta": {"mode": "precreate"},
    }

    with httpx.Client(base_url=api_url, timeout=30.0) as client:
        resp = _post_with_retry(
            client,
            "/api/sync/upload",
            payload=payload,
            headers={"Authorization": f"Bearer {api_key}"},
            max_retries=3,
            backoff_seconds=1.0,
            timeout_seconds=30.0,
        )
        upload_result = resp.json()

    run_batch_id = upload_result.get("run_batch_id")
    if run_batch_id:
        sync_state["last_run_batch_id"] = run_batch_id
        sync_state["last_experiment_id"] = experiment_id
        _write_sync_state(scenario_root, sync_state)

    if not quiet:
        console.print(
            f"[green]✓[/green] Precreated {len(runs)} runs (experiment: {experiment_id})"
        )
    return run_batch_id


def stream_turn(
    *,
    turn: Dict[str, Any],
    experiment_id: str,
    scenario_root: Path,
    api_url: Optional[str] = None,
    api_key: Optional[str] = None,
    endpoint: Optional[str] = None,
    max_retries: int = 3,
    backoff_seconds: float = 1.0,
    timeout_seconds: float = 10.0,
    quiet: bool = False,
) -> bool:
    """
    Stream a single turn to the sync API (best-effort).
    """
    try:
        api_url = resolve_api_url(api_url)
        api_key = _resolve_api_key(api_key)
        run_id = turn.get("run_id")
        turn_id = turn.get("turn_id")
        if not run_id or not turn_id:
            return False

        allowed_keys = {
            "run_id",
            "turn_id",
            "sequence",
            "role",
            "content",
            "timestamp",
            "duration_ms",
            "warnings",
        }
        payload: Dict[str, Any] = {key: turn.get(key) for key in allowed_keys}
        payload["run_id"] = run_id
        payload["turn_id"] = turn_id

        if payload.get("sequence") is not None:
            try:
                payload["sequence"] = int(payload["sequence"])
            except (TypeError, ValueError):
                pass

        if not payload.get("timestamp"):
            payload["timestamp"] = utc_now_iso()

        if payload.get("warnings") is None:
            payload["warnings"] = []

        idempotency_key = f"{run_id}:{turn_id}"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Idempotency-Key": idempotency_key,
        }
        if endpoint is None:
            endpoint = "/api/sync/turns"

        with httpx.Client(base_url=api_url, timeout=timeout_seconds) as client:
            _post_with_retry(
                client,
                endpoint,
                payload=payload,
                headers=headers,
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
                timeout_seconds=timeout_seconds,
            )
        if not quiet:
            console.print(f"[green][Stream][/green] ✓ {turn_id}")
        return True
    except Exception as exc:
        if not quiet:
            console.print(f"[yellow]Turn stream failed:[/yellow] {exc}")
        return False


def _get_state_dir(scenario_root: Path) -> Path:
    """Get the .state directory for a scenario."""
    return scenario_root / ".state"


def _ensure_state_dir(scenario_root: Path) -> Path:
    """Ensure .state directory exists."""
    state_dir = _get_state_dir(scenario_root)
    state_dir.mkdir(parents=True, exist_ok=True)
    return state_dir


# Alias for backward compatibility within this file
_ensure_fluxloop_dir = _ensure_state_dir


def _ensure_sync_dir(scenario_root: Path) -> Path:
    """Ensure .state/sync directory exists."""
    sync_dir = _ensure_state_dir(scenario_root) / "sync"
    sync_dir.mkdir(parents=True, exist_ok=True)
    return sync_dir


def _sync_state_paths(scenario_root: Path) -> List[Path]:
    """Get possible sync.json paths."""
    state_dir = _get_state_dir(scenario_root)
    return [
        state_dir / "sync.json",
        state_dir / "sync" / "sync.json",
    ]


def _read_sync_state(project_root: Path) -> Dict[str, Any]:
    for path in _sync_state_paths(project_root):
        if path.exists():
            try:
                return json.loads(path.read_text())
            except Exception:
                continue
    return {}


def _write_sync_state(project_root: Path, payload: Dict[str, Any]) -> None:
    for path in _sync_state_paths(project_root):
        _write_json(path, payload)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))


def _slugify(value: str) -> str:
    lowered = value.strip().lower()
    cleaned = "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in lowered)
    return cleaned.strip("_") or "criteria"


def _write_criteria(criteria_dir: Path, criteria_pack: List[Dict[str, Any]]) -> None:
    criteria_dir.mkdir(parents=True, exist_ok=True)
    for idx, item in enumerate(criteria_pack):
        if not isinstance(item, dict):
            continue
        name = item.get("name") or item.get("id") or f"criteria_{idx + 1}"
        filename = _slugify(str(name))
        path = criteria_dir / f"{filename}.yaml"
        path.write_text(
            yaml.safe_dump(item, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )


def _write_contracts(contracts_dir: Path, contracts: List[Dict[str, Any]]) -> None:
    """Write contracts to individual YAML files.
    
    Contract structure from backend:
    {
        "type": "must" | "should" | "must_not",
        "description": "...",
        "category": "response" | "safety" | "ux" | ...
    }
    """
    contracts_dir.mkdir(parents=True, exist_ok=True)
    for idx, item in enumerate(contracts):
        if not isinstance(item, dict):
            continue
        contract_type = item.get("type") or "contract"
        category = item.get("category") or ""
        if category:
            name = f"{contract_type}_{category}_{idx + 1}"
        else:
            name = f"{contract_type}_{idx + 1}"
        filename = _slugify(name)
        path = contracts_dir / f"{filename}.yaml"
        path.write_text(
            yaml.safe_dump(item, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )


def _extract_input_text(messages: Any) -> str:
    if isinstance(messages, list):
        for entry in messages:
            if isinstance(entry, dict):
                role = entry.get("role") or entry.get("type")
                if role in ("user", "human"):
                    content = entry.get("content")
                    if isinstance(content, str):
                        return content
        for entry in messages:
            if isinstance(entry, dict):
                content = entry.get("content")
                if isinstance(content, str):
                    return content
    if isinstance(messages, dict):
        content = messages.get("content")
        if isinstance(content, str):
            return content
    if isinstance(messages, str):
        return messages
    return ""


def _resolve_inputs_path(
    scenario: Optional[str], base_dir: Optional[Path], config_file: Path
) -> Tuple[Path, Optional[str]]:
    resolved = resolve_config_path(config_file, scenario)
    if resolved.exists():
        try:
            config = load_experiment_config(
                resolved, scenario=scenario, require_inputs_file=False
            )
            inputs_file = config.inputs_file or "inputs/generated.yaml"
            return (
                _resolve_relative_path(Path(inputs_file), scenario, base_dir),
                inputs_file,
            )
        except Exception:
            pass
    inputs_file = "inputs/generated.yaml"
    return _resolve_relative_path(Path(inputs_file), scenario, base_dir), inputs_file


def _load_inputs_mapping(inputs_path: Path) -> Dict[Tuple[str, Optional[str]], List[str]]:
    if not inputs_path.exists():
        return {}
    payload = yaml.safe_load(inputs_path.read_text()) or {}
    entries = payload.get("inputs") if isinstance(payload, dict) else payload
    if not isinstance(entries, list):
        return {}

    mapping: Dict[Tuple[str, Optional[str]], List[str]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        input_text = entry.get("input")
        metadata = entry.get("metadata") or {}
        item_id = metadata.get("input_item_id")
        if not input_text or not item_id:
            continue
        persona = entry.get("persona") or metadata.get("persona")
        mapping.setdefault((input_text, persona), []).append(item_id)
    return mapping


def _resolve_input_item_id(
    trace: dict, mapping: Dict[Tuple[str, Optional[str]], List[str]]
) -> Optional[str]:
    conversation_state = trace.get("conversation_state") or {}
    metadata = conversation_state.get("metadata") if isinstance(conversation_state, dict) else {}
    variation = metadata.get("variation") if isinstance(metadata, dict) else None
    if isinstance(variation, dict):
        direct = variation.get("input_item_id")
        if direct:
            return direct

    input_text = trace.get("input")
    persona = trace.get("persona")
    if input_text:
        key = (input_text, persona)
        if key in mapping and mapping[key]:
            return mapping[key].pop(0)
        key = (input_text, None)
        if key in mapping and mapping[key]:
            return mapping[key].pop(0)
        # Fallback: match by input text only when persona metadata is missing/mismatched.
        fallback_keys = [
            key for key in mapping.keys() if key[0] == input_text and mapping[key]
        ]
        if fallback_keys:
            fallback_keys.sort(key=lambda pair: "" if pair[1] is None else str(pair[1]))
            return mapping[fallback_keys[0]].pop(0)
    return None


def _coerce_run_id(trace_id: Optional[str]) -> str:
    if trace_id:
        try:
            return str(UUID(trace_id))
        except Exception:
            pass
    return str(uuid4())


def _iter_jsonl(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _guess_content_type(path: Path) -> str:
    if path.suffix in (".json", ".jsonl"):
        return "application/json"
    if path.suffix == ".md":
        return "text/markdown"
    if path.suffix == ".html":
        return "text/html"
    if path.suffix == ".pdf":
        return "application/pdf"
    return "application/octet-stream"


@app.command()
def pull(
    project_id: Optional[str] = typer.Option(None, "--project-id", help="Web Project ID"),
    bundle_version_id: Optional[str] = typer.Option(
        None, "--bundle-version-id", help="Bundle version ID"
    ),
    config_file: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config", "-c", help="Config file to resolve inputs path"
    ),
    scenario: Optional[str] = typer.Option(None, "--scenario", help="Scenario name (defaults to current context)"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Sync API base URL"),
    api_key: Optional[str] = typer.Option(None, "--api-key", help="Sync API key"),
    quiet: bool = typer.Option(False, "--quiet", help="Minimal output"),
):
    """
    Pull bundle inputs/personas/criteria for local execution.
    
    Data is saved to .fluxloop/scenarios/{scenario}/.
    Uses current scenario from context if --scenario is not specified.
    """
    # Resolve scenario from argument or context
    scenario = _get_effective_scenario(scenario)
    if scenario and not quiet:
        console.print(f"[dim]Using scenario: {scenario}[/dim]")
    
    _load_env(scenario)
    api_url = resolve_api_url(api_url)
    api_key = _resolve_api_key(api_key)

    if not project_id:
        project_id = get_current_web_project_id()
        if not project_id and not bundle_version_id:
            console.print("[yellow]No Web Project selected.[/yellow]")
            console.print("[dim]Select one with: fluxloop projects select <id>[/dim]")
            raise typer.Exit(1)

    payload: Dict[str, Any] = {
        "bundle_version_id": bundle_version_id,
        "project_id": project_id,
        "include_inputs": True,
        "include_personas": True,
        "include_criteria": True,
    }

    with httpx.Client(base_url=api_url, timeout=30.0) as client:
        resp = client.post(
            "/api/sync/pull",
            json=payload,
            headers={"Authorization": f"Bearer {api_key}"},
        )
        resp.raise_for_status()
        data = resp.json()

    scenario_root = _resolve_scenario_dir(scenario)
    state_dir = _ensure_state_dir(scenario_root)
    sync_dir = _ensure_sync_dir(scenario_root)

    personas = data.get("personas") or []
    persona_map = {item.get("id"): item.get("name") for item in personas if item.get("id")}

    inputs_path, inputs_file = _resolve_inputs_path(scenario, None, config_file)
    inputs_payload = {"inputs": []}
    for item in data.get("input_items") or []:
        input_text = _extract_input_text(item.get("messages"))
        if not input_text:
            continue
        metadata = dict(item.get("metadata") or {})
        metadata["input_item_id"] = item.get("id")
        persona_id = item.get("persona_id")
        if persona_id:
            metadata["persona_id"] = persona_id
            persona_name = persona_map.get(persona_id)
            if persona_name:
                metadata.setdefault("persona", persona_name)
        inputs_payload["inputs"].append(
            {
                "input": input_text,
                "metadata": metadata,
            }
        )

    inputs_path.parent.mkdir(parents=True, exist_ok=True)
    inputs_path.write_text(yaml.safe_dump(inputs_payload, sort_keys=False, allow_unicode=True))

    criteria_pack = data.get("criteria_pack") or []
    _write_json(sync_dir / "personas.json", {"items": personas})
    _write_json(sync_dir / "criteria.json", {"items": criteria_pack})
    _write_criteria(state_dir / "criteria", criteria_pack if isinstance(criteria_pack, list) else [])

    # Save scenario_context (scenario info + contracts)
    scenario_context = data.get("scenario_context") or {}
    scenario_info = scenario_context.get("scenario") if isinstance(scenario_context, dict) else {}
    contracts = scenario_context.get("contracts", []) if isinstance(scenario_context, dict) else []
    if scenario_info:
        _write_json(sync_dir / "scenario.json", scenario_info)
    if contracts:
        _write_json(sync_dir / "contracts.json", {"items": contracts})
        _write_contracts(state_dir / "contracts", contracts if isinstance(contracts, list) else [])

    sync_state = {
        "project_id": data.get("bundle_version", {}).get("project_id") or project_id,
        "bundle_version_id": data.get("bundle_version", {}).get("id"),
        "scenario_id": scenario_info.get("id") if scenario_info else None,
        "inputs_file": inputs_file,
        "pulled_at": data.get("sync_meta", {}).get("pulled_at"),
        "api_url": api_url,
    }
    _write_sync_state(scenario_root, sync_state)

    if not quiet:
        console.print(f"[green]✓[/green] Pulled {len(inputs_payload['inputs'])} inputs")
        if contracts:
            console.print(f"[green]✓[/green] Pulled {len(contracts)} contracts")
        console.print(f"[green]✓[/green] Saved inputs to {inputs_path}")
        console.print(f"[green]✓[/green] Saved sync metadata to {_sync_state_paths(scenario_root)[0]}")


@app.command()
def upload(
    experiment_dir: Optional[Path] = typer.Option(
        None, "--experiment-dir", help="Experiment output directory"
    ),
    config_file: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config", "-c", help="Config file to resolve output dir"
    ),
    scenario: Optional[str] = typer.Option(None, "--scenario", help="Scenario name (defaults to current context)"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="Sync API base URL"),
    api_key: Optional[str] = typer.Option(None, "--api-key", help="Sync API key"),
    bundle_version_id: Optional[str] = typer.Option(
        None, "--bundle-version-id", help="Bundle version ID"
    ),
    quiet: bool = typer.Option(False, "--quiet", help="Minimal output"),
):
    """
    Upload run results and artifacts to the sync API.
    
    Uses current scenario from context if --scenario is not specified.
    """
    # Resolve scenario from argument or context
    scenario = _get_effective_scenario(scenario)
    if scenario and not quiet:
        console.print(f"[dim]Using scenario: {scenario}[/dim]")
    
    _load_env(scenario)
    api_url = resolve_api_url(api_url)
    api_key = _resolve_api_key(api_key)

    scenario_root = _resolve_scenario_dir(scenario)
    sync_state = _read_sync_state(scenario_root)

    bundle_version_id = bundle_version_id or sync_state.get("bundle_version_id")
    if not bundle_version_id:
        raise typer.BadParameter("bundle_version_id is required. Run sync pull first.")

    resolved_config = resolve_config_path(config_file, scenario)
    config = load_experiment_config(
        resolved_config, scenario=scenario, require_inputs_file=False
    )
    output_base = _resolve_relative_path(Path(config.output_directory), scenario)

    if experiment_dir is None:
        if not output_base.exists():
            raise typer.BadParameter(f"Experiment output directory not found: {output_base}")
        candidates = [
            path
            for path in output_base.iterdir()
            if path.is_dir() and path.name != "artifacts"
        ]
        if not candidates:
            raise typer.BadParameter(f"No experiment directories found in {output_base}")
        experiment_dir = sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)[0]
    else:
        experiment_dir = experiment_dir.expanduser().resolve()

    trace_summary_path = experiment_dir / "trace_summary.jsonl"
    if not trace_summary_path.exists():
        raise typer.BadParameter(f"trace_summary.jsonl not found: {trace_summary_path}")

    # Generate experiment_id from directory name for idempotent upload
    experiment_id = experiment_dir.name

    inputs_path, _ = _resolve_inputs_path(scenario, None, config_file)
    input_map = _load_inputs_mapping(inputs_path)

    traces = list(_iter_jsonl(trace_summary_path))
    run_ids: Dict[str, str] = {}
    runs_payload: List[dict] = []
    results_payload: List[dict] = []
    all_completed = True

    for trace in traces:
        trace_id = trace.get("trace_id")
        run_id = _coerce_run_id(trace_id)
        run_ids[trace_id or run_id] = run_id

        input_item_id = _resolve_input_item_id(trace, input_map)
        if not input_item_id:
            raise typer.BadParameter(
                f"input_item_id not found for trace {trace_id}. Run sync pull first."
            )

        status = "completed" if trace.get("success") else "failed"
        if status != "completed":
            all_completed = False

        runs_payload.append(
            {
                "id": run_id,
                "input_item_id": input_item_id,
                "persona_id": None,
                "status": status,
            }
        )

        metrics = {}
        if trace.get("duration_ms") is not None:
            metrics["duration_ms"] = trace.get("duration_ms")
        if trace.get("token_usage"):
            metrics.update(trace.get("token_usage"))

        transcript = []
        for entry in trace.get("conversation") or []:
            if not isinstance(entry, dict):
                continue
            transcript.append(
                {
                    "role": entry.get("role"),
                    "content": entry.get("content"),
                    "metadata": entry.get("metadata"),
                }
            )

        local_eval_summary = {
            "verdict": "pass" if trace.get("success") else "fail",
            "scores": {},
            "failures": [],
        }

        results_payload.append(
            {
                "run_id": run_id,
                "status": status,
                "metrics": metrics,
                "transcript": transcript,
                "local_eval_summary": local_eval_summary,
            }
        )

    run_batch_status = "completed" if all_completed else "failed"

    artifacts_payload: List[dict] = []
    project_id = sync_state.get("project_id")

    def _upload_file(run_id: str, artifact_type: str, file_path: Path) -> None:
        if not file_path.exists():
            return
        content_type = _guess_content_type(file_path)
        with httpx.Client(base_url=api_url, timeout=60.0) as client:
            presign_resp = client.post(
                "/api/storage/presign",
                json={
                    "project_id": project_id,
                    "run_id": run_id,
                    "artifact_type": artifact_type,
                    "filename": file_path.name,
                    "content_type": content_type,
                },
                headers={"Authorization": f"Bearer {api_key}"},
            )
            presign_resp.raise_for_status()
            presign = presign_resp.json()

            upload_headers = presign.get("headers") or {}
            upload_url = presign["upload_url"]
            upload_resp = httpx.put(
                upload_url,
                content=file_path.read_bytes(),
                headers=upload_headers,
                timeout=120.0,
            )
            upload_resp.raise_for_status()
            artifacts_payload.append(
                {
                    "run_id": run_id,
                    "type": artifact_type,
                    "storage_url": presign["storage_url"],
                }
            )

    per_trace_dir = experiment_dir / "per_trace_analysis"
    if per_trace_dir.exists():
        for trace in traces:
            trace_id = trace.get("trace_id")
            if not trace_id:
                continue
            run_id = run_ids.get(trace_id)
            if not run_id:
                continue
            matches = list(per_trace_dir.glob(f"*{trace_id}*.md"))
            if matches:
                _upload_file(run_id, "per_trace_markdown", matches[0])

        per_trace_jsonl = per_trace_dir / "per_trace.jsonl"
        if traces:
            first_run_id = run_ids.get(traces[0].get("trace_id"), list(run_ids.values())[0])
            _upload_file(first_run_id, "per_trace_index", per_trace_jsonl)

    if traces:
        first_run_id = run_ids.get(traces[0].get("trace_id"), list(run_ids.values())[0])
        _upload_file(first_run_id, "summary", experiment_dir / "summary.json")
        _upload_file(first_run_id, "trace_summary", experiment_dir / "trace_summary.jsonl")
        _upload_file(first_run_id, "observations", experiment_dir / "observations.jsonl")

    turns_payload = load_turns(experiment_dir / "turns.jsonl")
    turn_summaries = summarize_turns(turns_payload) if turns_payload else {}
    turn_summary_payload: List[Dict[str, Any]] = []
    for run_id in run_ids.values():
        summary = turn_summaries.get(run_id)
        if summary is None:
            continue
        turn_summary_payload.append(
            {
                "run_id": run_id,
                "total_turns": summary.total_turns,
                "warning_turns": summary.warning_turns,
                "warning_count": summary.warning_count,
                "warning_rate": summary.warning_rate,
            }
        )

    payload = {
        "bundle_version_id": bundle_version_id,
        "run_batch": {
            "experiment_id": experiment_id,
            "execution_target": "local",
            "status": run_batch_status,
        },
        "runs": runs_payload,
        "run_results": results_payload,
        "artifacts": artifacts_payload,
        "turns": turns_payload,
        "turn_summaries": turn_summary_payload,
        "upload_meta": {},
    }

    with httpx.Client(base_url=api_url, timeout=60.0) as client:
        resp = client.post(
            "/api/sync/upload",
            json=payload,
            headers={"Authorization": f"Bearer {api_key}"},
        )
        resp.raise_for_status()
        upload_result = resp.json()

    # Cache run_batch_id for future reference
    run_batch_id = upload_result.get("run_batch_id")
    if run_batch_id:
        sync_state["last_run_batch_id"] = run_batch_id
        sync_state["last_experiment_id"] = experiment_id
        _write_sync_state(scenario_root, sync_state)

    if not quiet:
        console.print(
            f"[green]✓[/green] Uploaded {len(runs_payload)} runs (experiment: {experiment_id})"
        )
