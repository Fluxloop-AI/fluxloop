"""
Project data management commands for FluxLoop CLI.

Provides commands for uploading, listing, and managing project data (Knowledge/Dataset).
Implements the data push → confirm → (optional) bind workflow.
"""

from __future__ import annotations

import hashlib
import json
import mimetypes
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import typer
from click.core import ParameterSource
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from ..api_utils import handle_api_error, resolve_api_url
from ..context_manager import (
    get_current_scenario_id,
    get_current_web_project_id,
)
from ..http_client import create_authenticated_client

app = typer.Typer(help="Manage project data (Knowledge & Datasets)")
gt_app = typer.Typer(help="Manage Ground Truth materialization and status")
app.add_typer(gt_app, name="gt")
console = Console()


# Extension-based auto-detection for data category
DATASET_EXTENSIONS = {".csv", ".json", ".jsonl", ".xlsx", ".xls", ".tsv"}
DOCUMENT_EXTENSIONS = {".pdf", ".docx", ".doc", ".md", ".txt", ".html", ".htm"}

USAGE_CONTEXT = "context"
USAGE_GROUND_TRUTH = "ground-truth"
VALID_GT_SPLITS = {"train", "dev", "test"}
DEFAULT_GT_SAMPLING_SEED = 42


def _infer_data_category(file_path: Path, override: Optional[str] = None) -> str:
    """
    Infer data category from file extension or explicit override.

    Args:
        file_path: Path to the file.
        override: Explicit category override ('document' or 'dataset').

    Returns:
        'KNOWLEDGE' for documents, 'DATASET' for structured data.
    """
    if override:
        override_lower = override.lower()
        if override_lower == "dataset":
            return "DATASET"
        elif override_lower in {"document", "knowledge"}:
            return "KNOWLEDGE"

    ext = file_path.suffix.lower()
    if ext in DATASET_EXTENSIONS:
        return "DATASET"
    return "KNOWLEDGE"


def _infer_mime_type(file_path: Path) -> str:
    """Infer MIME type from file extension."""
    mime_type, _ = mimetypes.guess_type(str(file_path))
    return mime_type or "application/octet-stream"


def _compute_content_hash(file_path: Path) -> str:
    """Compute SHA-256 hash of file contents."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def _infer_file_type(data_category: str) -> str:
    """
    Infer API file_type enum from data category.

    API accepts: 'document', 'structured', 'sample'.
    For CLI uploads:
    - KNOWLEDGE -> document
    - DATASET   -> structured
    """
    if data_category == "DATASET":
        return "structured"
    return "document"


def _normalize_usage(usage: str) -> str:
    usage_norm = usage.strip().lower()
    if usage_norm not in {USAGE_CONTEXT, USAGE_GROUND_TRUTH}:
        raise typer.BadParameter(
            "--usage must be one of: context, ground-truth"
        )
    return usage_norm


def _normalize_split(split: Optional[str]) -> Optional[str]:
    if split is None:
        return None
    split_norm = split.strip().lower()
    if split_norm not in VALID_GT_SPLITS:
        raise typer.BadParameter("--split must be one of: train, dev, test")
    return split_norm


def _normalize_role(role: Optional[str]) -> Optional[str]:
    if role is None:
        return None
    role_norm = role.strip().lower()
    return role_norm or None


def _gt_options_used(
    *,
    split: Optional[str],
    label_column: Optional[str],
    row_filter: Optional[str],
    sampling_seed: int,
    sampling_seed_explicit: bool = False,
) -> bool:
    return bool(
        split
        or label_column
        or row_filter
        or sampling_seed_explicit
        or sampling_seed != DEFAULT_GT_SAMPLING_SEED
    )


def _was_option_explicitly_set(ctx: Optional[typer.Context], name: str) -> bool:
    if ctx is None:
        return False
    try:
        source = ctx.get_parameter_source(name)
    except Exception:
        return False
    if source is None:
        return False
    return source not in {ParameterSource.DEFAULT, ParameterSource.DEFAULT_MAP}


def _extract_error_detail(resp: httpx.Response) -> str:
    try:
        payload = resp.json()
    except Exception:
        return resp.text.strip() or "No error detail provided."

    detail: Any = payload
    if isinstance(payload, dict) and "detail" in payload:
        detail = payload.get("detail")

    if isinstance(detail, str):
        return detail
    if isinstance(detail, dict):
        code = detail.get("code")
        message = (
            detail.get("message")
            or detail.get("detail")
            or detail.get("error")
        )
        if isinstance(code, str) and isinstance(message, str):
            return f"{code}: {message}"
        if isinstance(message, str):
            return message
    try:
        return json.dumps(detail, ensure_ascii=False, default=str)
    except Exception:
        return str(detail)


def _print_materialize_error(
    *,
    resp: httpx.Response,
    scenario_id: str,
    data_id: str,
) -> None:
    detail = _extract_error_detail(resp)
    detail_lower = detail.lower()

    console.print(
        f"[red]✗[/red] Ground Truth materialization failed ({resp.status_code})",
        style="bold red",
    )
    console.print(f"  API detail: {detail}")
    console.print("  Next actions:")

    if resp.status_code == 409 and any(
        token in detail_lower for token in ("processing", "not ready", "pending", "queued")
    ):
        console.print(f"  1) Wait for dataset processing: fluxloop data show {data_id}")
        console.print(
            "  2) Retry materialization once processing is completed "
            f"(fluxloop data bind {data_id} --scenario {scenario_id} --role validation)"
        )
        return

    if resp.status_code == 409 and any(
        token in detail_lower for token in ("role", "validation")
    ):
        console.print(
            "  1) Ensure validation role binding: "
            f"fluxloop data bind {data_id} --scenario {scenario_id} --role validation"
        )
        console.print(
            "  2) Verify current GT state: "
            f"fluxloop data gt status --scenario {scenario_id} --data-id {data_id}"
        )
        return

    console.print(f"  1) Inspect processing state: fluxloop data show {data_id}")
    console.print(
        "  2) Check GT status and retry as needed: "
        f"fluxloop data gt status --scenario {scenario_id} --data-id {data_id}"
    )


def _extract_profile_id(payload: Dict[str, Any]) -> Optional[str]:
    profile = payload.get("profile")
    if isinstance(profile, dict):
        profile_id = profile.get("id")
        if isinstance(profile_id, str) and profile_id:
            return profile_id

    profile_id = payload.get("ground_truth_profile_id")
    if isinstance(profile_id, str) and profile_id:
        return profile_id

    binding = payload.get("binding")
    if isinstance(binding, dict):
        binding_meta = binding.get("binding_meta")
        if isinstance(binding_meta, dict):
            nested_profile_id = binding_meta.get("ground_truth_profile_id")
            if isinstance(nested_profile_id, str) and nested_profile_id:
                return nested_profile_id

    return None


def _extract_gt_contract_ids(payload: Dict[str, Any]) -> List[str]:
    ids: List[str] = []

    contracts = payload.get("gt_contracts")
    if isinstance(contracts, list):
        for contract in contracts:
            if isinstance(contract, str) and contract:
                ids.append(contract)
            elif isinstance(contract, dict):
                contract_id = contract.get("id") or contract.get("contract_id")
                if isinstance(contract_id, str) and contract_id:
                    ids.append(contract_id)

    contract_ids = payload.get("gt_contract_ids")
    if isinstance(contract_ids, list):
        for contract_id in contract_ids:
            if isinstance(contract_id, str) and contract_id:
                ids.append(contract_id)

    unique_ids: List[str] = []
    seen = set()
    for contract_id in ids:
        if contract_id not in seen:
            seen.add(contract_id)
            unique_ids.append(contract_id)
    return unique_ids


def _materialize_ground_truth(
    *,
    client: Any,
    scenario_id: str,
    data_id: str,
    split: Optional[str],
    label_column: Optional[str],
    row_filter: Optional[str],
    sampling_seed: int,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "data_id": data_id,
        "sampling_seed": sampling_seed,
    }
    if split:
        payload["split"] = split
    if label_column:
        payload["label_column"] = label_column
    if row_filter:
        payload["row_filter"] = row_filter

    resp = client.post(
        f"/api/scenarios/{scenario_id}/ground-truth/materialize",
        json=payload,
    )
    if not resp.is_success:
        _print_materialize_error(resp=resp, scenario_id=scenario_id, data_id=data_id)
        raise typer.Exit(1)

    parsed = resp.json()
    return parsed if isinstance(parsed, dict) else {}


def _build_gt_status_rows(
    payload: Any,
    *,
    fallback_data_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    raw_items: List[Any]
    if isinstance(payload, list):
        raw_items = payload
    elif isinstance(payload, dict):
        if isinstance(payload.get("items"), list):
            raw_items = payload.get("items", [])
        elif isinstance(payload.get("statuses"), list):
            raw_items = payload.get("statuses", [])
        else:
            raw_items = [payload]
    else:
        raw_items = []

    rows: List[Dict[str, Any]] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue

        item_data_id = item.get("data_id")
        if not isinstance(item_data_id, str) or not item_data_id:
            item_data_id = fallback_data_id or "N/A"

        profile_id = item.get("ground_truth_profile_id")
        if not isinstance(profile_id, str):
            profile_id = None
        if not profile_id:
            profile = item.get("profile")
            if isinstance(profile, dict):
                nested_profile_id = profile.get("id")
                if isinstance(nested_profile_id, str) and nested_profile_id:
                    profile_id = nested_profile_id

        contract_ids: List[str] = []
        gt_contract_ids = item.get("gt_contract_ids")
        if isinstance(gt_contract_ids, list):
            contract_ids = [c for c in gt_contract_ids if isinstance(c, str) and c]
        elif isinstance(item.get("gt_contracts"), list):
            for contract in item.get("gt_contracts", []):
                if isinstance(contract, str) and contract:
                    contract_ids.append(contract)
                elif isinstance(contract, dict):
                    contract_id = contract.get("id")
                    if isinstance(contract_id, str) and contract_id:
                        contract_ids.append(contract_id)

        rows.append(
            {
                "data_id": item_data_id,
                "materialization_status": item.get("materialization_status", "unknown"),
                "ground_truth_profile_id": profile_id or "-",
                "gt_contract_count": len(contract_ids),
                "processing_status": item.get("processing_status", "unknown"),
                "updated_at": item.get("updated_at", "-"),
                "gt_contract_ids": contract_ids,
            }
        )

    return rows


def _resolve_push_scenario_id(
    *,
    usage_mode: str,
    scenario: Optional[str],
    bind: bool,
) -> Optional[str]:
    scenario_id = scenario
    if bind and not scenario_id:
        scenario_id = get_current_scenario_id()

    if usage_mode == USAGE_GROUND_TRUTH and not scenario_id:
        console.print(
            "[red]✗[/red] Ground Truth upload requires a scenario binding.",
            style="bold red",
        )
        console.print(
            "[dim]Provide --scenario <id> or set current scenario then use --bind.[/dim]"
        )
        raise typer.Exit(1)

    return scenario_id


def _bind_after_push(
    *,
    client: Any,
    scenario_id: str,
    data_id: str,
    usage_mode: str,
    split_value: Optional[str],
    label_column: Optional[str],
    row_filter: Optional[str],
    sampling_seed: int,
    materialize_gt: bool,
    quiet: bool,
) -> Optional[Dict[str, Any]]:
    materialize_result: Optional[Dict[str, Any]] = None

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task("Binding to scenario...", total=None)

        bind_payload: Dict[str, Any] = {"data_id": data_id}
        if usage_mode == USAGE_GROUND_TRUTH:
            binding_meta: Dict[str, Any] = {
                "role": "validation",
                "sampling_seed": sampling_seed,
            }
            if split_value:
                binding_meta["split"] = split_value
            if label_column:
                binding_meta["label_column"] = label_column
            if row_filter:
                binding_meta["row_filter"] = row_filter
            bind_payload["binding_meta"] = binding_meta

        bind_resp = client.post(
            f"/api/scenarios/{scenario_id}/data/bind",
            json=bind_payload,
        )

        if usage_mode == USAGE_GROUND_TRUTH:
            if bind_resp.status_code == 409:
                if not quiet:
                    console.print("[dim]Already bound to scenario[/dim]")
            else:
                handle_api_error(bind_resp, "data binding")
                if not quiet:
                    console.print(f"[green]✓[/green] Bound to scenario: {scenario_id}")

            if materialize_gt:
                progress.update(task, description="Materializing Ground Truth...")
                materialize_result = _materialize_ground_truth(
                    client=client,
                    scenario_id=scenario_id,
                    data_id=data_id,
                    split=split_value,
                    label_column=label_column,
                    row_filter=row_filter,
                    sampling_seed=sampling_seed,
                )
            elif not quiet:
                console.print("[dim]Skipped GT materialization (--no-materialize-gt)[/dim]")
        else:
            if bind_resp.status_code == 404:
                console.print(f"[yellow]⚠[/yellow] Scenario not found: {scenario_id}")
            elif bind_resp.status_code == 409:
                if not quiet:
                    console.print("[dim]Already bound to scenario[/dim]")
            elif bind_resp.is_success:
                if not quiet:
                    console.print(f"[green]✓[/green] Bound to scenario: {scenario_id}")
            else:
                console.print(f"[yellow]⚠[/yellow] Binding failed: {bind_resp.status_code}")

    return materialize_result


@app.command()
def push(
    ctx: typer.Context,
    file: Path = typer.Argument(..., help="File to upload to project data library"),
    as_type: Optional[str] = typer.Option(
        None,
        "--as",
        help="Data category: 'document' (Knowledge) or 'dataset'. Auto-detected if not specified.",
    ),
    scenario: Optional[str] = typer.Option(
        None,
        "--scenario",
        help="Scenario ID to bind after upload (uses current context if not specified)",
    ),
    bind: bool = typer.Option(
        False,
        "--bind",
        help="Bind to current scenario after upload",
    ),
    usage: str = typer.Option(
        USAGE_CONTEXT,
        "--usage",
        help="Data usage mode: context or ground-truth",
    ),
    split: Optional[str] = typer.Option(
        None,
        "--split",
        help="Ground Truth split (train|dev|test). Only for --usage ground-truth.",
    ),
    label_column: Optional[str] = typer.Option(
        None,
        "--label-column",
        help="Ground Truth label column. Only for --usage ground-truth.",
    ),
    row_filter: Optional[str] = typer.Option(
        None,
        "--row-filter",
        help="Ground Truth row filter expression. Only for --usage ground-truth.",
    ),
    sampling_seed: int = typer.Option(
        DEFAULT_GT_SAMPLING_SEED,
        "--sampling-seed",
        help="Ground Truth sampling seed (default: 42). Only for --usage ground-truth.",
    ),
    materialize_gt: bool = typer.Option(
        True,
        "--materialize-gt/--no-materialize-gt",
        help="Materialize Ground Truth profile/contracts after bind (GT mode).",
    ),
    project_id: Optional[str] = typer.Option(
        None,
        "--project-id",
        help="Project ID (defaults to current context)",
    ),
    api_url: Optional[str] = typer.Option(
        None,
        "--api-url",
        help="FluxLoop API base URL",
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Minimal output"),
):
    """
    Upload a file to the project data library.

    The file is automatically categorized as KNOWLEDGE (documents) or DATASET
    based on its extension. Use --as to override the auto-detection.

    Examples:
        # Upload a document (auto-detected)
        fluxloop data push spec.pdf

        # Upload as dataset explicitly
        fluxloop data push users.csv --as dataset

        # Upload and bind to current scenario
        fluxloop data push test_data.json --bind

        # Upload and bind to specific scenario
        fluxloop data push requirements.md --scenario abc123
    """
    usage_mode = _normalize_usage(usage)
    split_value = _normalize_split(split)
    sampling_seed_explicit = _was_option_explicitly_set(ctx, "sampling_seed")
    gt_options_requested = _gt_options_used(
        split=split_value,
        label_column=label_column,
        row_filter=row_filter,
        sampling_seed=sampling_seed,
        sampling_seed_explicit=sampling_seed_explicit,
    )

    if usage_mode == USAGE_CONTEXT and gt_options_requested:
        raise typer.BadParameter(
            "--split/--label-column/--row-filter/--sampling-seed are only valid with --usage ground-truth"
        )

    if usage_mode == USAGE_GROUND_TRUTH and not (bind or scenario):
        raise typer.BadParameter(
            "--usage ground-truth requires --bind or --scenario"
        )

    scenario_id = _resolve_push_scenario_id(
        usage_mode=usage_mode,
        scenario=scenario,
        bind=bind,
    )

    # Validate file exists
    file = file.expanduser().resolve()
    if not file.exists():
        console.print(f"[red]✗[/red] File not found: {file}")
        raise typer.Exit(1)
    if not file.is_file():
        console.print(f"[red]✗[/red] Not a file: {file}")
        raise typer.Exit(1)

    # Resolve project
    api_url = resolve_api_url(api_url, staging=staging)
    if not project_id:
        project_id = get_current_web_project_id()
        if not project_id:
            console.print("[yellow]No Web Project selected.[/yellow]")
            console.print("[dim]Select one with: fluxloop projects select <id>[/dim]")
            raise typer.Exit(1)

    # Get file metadata
    filename = file.name
    file_size = file.stat().st_size
    mime_type = _infer_mime_type(file)

    if usage_mode == USAGE_GROUND_TRUTH:
        data_category = "DATASET"
        file_type = "structured"
        processing_profile = "dataset"
    else:
        data_category = _infer_data_category(file, as_type)
        file_type = _infer_file_type(data_category)
        processing_profile = "auto"
    category_display = "Dataset" if data_category == "DATASET" else "Document"

    if not quiet:
        console.print(f"[cyan]Uploading {filename}...[/cyan]")
        console.print(f"  Type: {category_display} ({data_category})")
        console.print(f"  Usage: {usage_mode}")
        console.print(f"  Size: {file_size:,} bytes")

    try:
        client = create_authenticated_client(api_url, use_jwt=True)

        # Step 1: Create data record and get upload URL
        create_payload = {
            "filename": filename,
            "file_type": file_type,
            "mime_type": mime_type,
            "file_size": file_size,
            "data_category": data_category,
            "processing_profile": processing_profile,
        }

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True,
        ) as progress:
            task = progress.add_task("Creating upload...", total=None)

            resp = client.post(
                f"/api/projects/{project_id}/data",
                json=create_payload,
            )
            handle_api_error(resp, "data upload")
            create_result = resp.json()

            data_record = create_result.get("data", {})
            data_id = data_record.get("id")
            upload_info = create_result.get("upload", {})
            upload_url = upload_info.get("upload_url")
            upload_headers = upload_info.get("headers") or {}

            if not data_id or not upload_url:
                console.print("[red]✗[/red] Failed to get upload URL")
                raise typer.Exit(1)

            progress.update(task, description="Uploading file...")

            # Step 2: Upload file to signed URL
            with open(file, "rb") as f:
                file_bytes = f.read()

            # Compute content hash
            content_hash = hashlib.sha256(file_bytes).hexdigest()

            upload_resp = httpx.put(
                upload_url,
                content=file_bytes,
                headers=upload_headers,
                timeout=120.0,
            )
            if not upload_resp.is_success:
                console.print(f"[red]✗[/red] Upload failed: {upload_resp.status_code}")
                raise typer.Exit(1)

            progress.update(task, description="Confirming upload...")

            # Step 3: Confirm upload
            confirm_payload = {
                "file_size": file_size,
                "mime_type": mime_type,
                "content_hash": content_hash,
            }

            confirm_resp = client.post(
                f"/api/projects/{project_id}/data/{data_id}/confirm",
                json=confirm_payload,
            )
            handle_api_error(confirm_resp, "upload confirmation")
            confirmed_data = confirm_resp.json()

        if not quiet:
            console.print(f"[green]✓[/green] Uploaded: {filename}")
            console.print(f"  Data ID: [bold]{data_id}[/bold]")
            console.print(f"  Status: {confirmed_data.get('processing_status', 'queued')}")

        # Step 4: (Optional) Bind to scenario
        materialize_result: Optional[Dict[str, Any]] = None
        if scenario_id:
            materialize_result = _bind_after_push(
                client=client,
                scenario_id=scenario_id,
                data_id=data_id,
                usage_mode=usage_mode,
                split_value=split_value,
                label_column=label_column,
                row_filter=row_filter,
                sampling_seed=sampling_seed,
                materialize_gt=materialize_gt,
                quiet=quiet,
            )

        if usage_mode == USAGE_GROUND_TRUTH:
            profile_id = _extract_profile_id(materialize_result or {})
            gt_contract_ids = _extract_gt_contract_ids(materialize_result or {})
            gt_contract_count = len(gt_contract_ids)
            if quiet:
                console.print(f"data_id={data_id}")
                console.print(f"scenario_id={scenario_id or '-'}")
                console.print(f"profile_id={profile_id or '-'}")
                console.print(f"gt_contract_count={gt_contract_count}")
            else:
                console.print("[green]✓[/green] Ground Truth binding complete")
                console.print(f"  data_id: [bold]{data_id}[/bold]")
                console.print(f"  scenario_id: [bold]{scenario_id}[/bold]")
                console.print(f"  profile_id: [bold]{profile_id or '-'}[/bold]")
                console.print(f"  gt_contract_count: [bold]{gt_contract_count}[/bold]")
        elif quiet:
            console.print(f"data_id={data_id}")
            if scenario_id:
                console.print(f"scenario_id={scenario_id}")

    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"[red]✗[/red] Upload failed: {e}", style="bold red")
        raise typer.Exit(1)


@app.command("list")
def list_data(
    project_id: Optional[str] = typer.Option(
        None,
        "--project-id",
        help="Project ID (defaults to current context)",
    ),
    category: Optional[str] = typer.Option(
        None,
        "--category",
        help="Filter by category: 'document' or 'dataset'",
    ),
    format: str = typer.Option(
        "table", "--format", help="Output format (table, json)"
    ),
    api_url: Optional[str] = typer.Option(
        None,
        "--api-url",
        help="FluxLoop API base URL",
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    List all data in the project library.

    Shows both KNOWLEDGE (documents) and DATASET entries with their processing status.
    """
    api_url = resolve_api_url(api_url, staging=staging)

    if not project_id:
        project_id = get_current_web_project_id()
        if not project_id:
            console.print("[yellow]No Web Project selected.[/yellow]")
            console.print("[dim]Select one with: fluxloop projects select <id>[/dim]")
            raise typer.Exit(1)

    try:
        client = create_authenticated_client(api_url, use_jwt=True)

        resp = client.get(f"/api/projects/{project_id}/data")
        handle_api_error(resp, "project data list")

        data = resp.json()
        items = data.get("items", [])

        # Filter by category if specified
        if category:
            category_filter = "DATASET" if category.lower() == "dataset" else "KNOWLEDGE"
            items = [item for item in items if item.get("data_category") == category_filter]

        if not items:
            console.print("[yellow]No data found.[/yellow]")
            console.print("[dim]Upload with: fluxloop data push <file>[/dim]")
            return

        if format == "json":
            import json

            console.print_json(json.dumps(items, ensure_ascii=False, default=str))
            return

        # Create table
        table = Table(title=f"Project Data Library ({len(items)} items)")
        table.add_column("ID", style="cyan", max_width=12)
        table.add_column("Filename", style="bold")
        table.add_column("Category", style="magenta")
        table.add_column("Status", style="green")
        table.add_column("Size", justify="right")

        for item in items:
            data_id = item.get("id", "N/A")
            # Truncate ID for display
            display_id = data_id[:8] + "..." if len(data_id) > 11 else data_id

            filename = item.get("filename") or "N/A"
            category_val = item.get("data_category", "KNOWLEDGE")
            category_display = "Dataset" if category_val == "DATASET" else "Document"
            status = item.get("processing_status", "unknown")

            # Format file size
            file_size = item.get("file_size")
            if file_size:
                if file_size >= 1024 * 1024:
                    size_str = f"{file_size / (1024 * 1024):.1f} MB"
                elif file_size >= 1024:
                    size_str = f"{file_size / 1024:.1f} KB"
                else:
                    size_str = f"{file_size} B"
            else:
                size_str = "-"

            # Color status
            status_style = {
                "completed": "[green]completed[/green]",
                "processing": "[yellow]processing[/yellow]",
                "queued": "[blue]queued[/blue]",
                "pending": "[dim]pending[/dim]",
                "failed": "[red]failed[/red]",
            }.get(status, status)

            table.add_row(display_id, filename, category_display, status_style, size_str)

        console.print(table)
        console.print()
        console.print("[dim]View details: fluxloop data show <id>[/dim]")

    except Exception as e:
        console.print(f"[red]✗[/red] List failed: {e}", style="bold red")
        raise typer.Exit(1)


@app.command()
def show(
    data_id: str = typer.Argument(..., help="Data ID to show details"),
    project_id: Optional[str] = typer.Option(
        None,
        "--project-id",
        help="Project ID (defaults to current context)",
    ),
    api_url: Optional[str] = typer.Option(
        None,
        "--api-url",
        help="FluxLoop API base URL",
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    Show details of a specific data record.
    """
    api_url = resolve_api_url(api_url, staging=staging)

    if not project_id:
        project_id = get_current_web_project_id()
        if not project_id:
            console.print("[yellow]No Web Project selected.[/yellow]")
            console.print("[dim]Select one with: fluxloop projects select <id>[/dim]")
            raise typer.Exit(1)

    try:
        client = create_authenticated_client(api_url, use_jwt=True)

        # Get data list and find the matching record
        # (API doesn't have a direct GET by ID, so we list and filter)
        resp = client.get(f"/api/projects/{project_id}/data")
        handle_api_error(resp, "project data")

        data = resp.json()
        items = data.get("items", [])

        # Find matching record (support partial ID match)
        matching = [
            item for item in items
            if item.get("id", "").startswith(data_id) or item.get("id") == data_id
        ]

        if not matching:
            console.print(f"[red]✗[/red] Data not found: {data_id}")
            raise typer.Exit(1)

        if len(matching) > 1:
            console.print(f"[yellow]⚠[/yellow] Multiple matches found. Please use a longer ID prefix.")
            for item in matching[:5]:
                console.print(f"  - {item.get('id')}: {item.get('filename')}")
            raise typer.Exit(1)

        item = matching[0]

        # Display details
        console.print()
        console.print(f"[bold blue]Data: {item.get('id')}[/bold blue]")
        console.print()

        console.print(f"[bold]Filename:[/bold] {item.get('filename', 'N/A')}")
        console.print(f"[bold]Category:[/bold] {item.get('data_category', 'KNOWLEDGE')}")
        console.print(f"[bold]Status:[/bold] {item.get('processing_status', 'unknown')}")

        file_size = item.get("file_size")
        if file_size:
            console.print(f"[bold]Size:[/bold] {file_size:,} bytes")

        mime_type = item.get("mime_type")
        if mime_type:
            console.print(f"[bold]MIME Type:[/bold] {mime_type}")

        content_hash = item.get("content_hash")
        if content_hash:
            console.print(f"[bold]Content Hash:[/bold] {content_hash[:16]}...")

        created_at = item.get("created_at")
        if created_at:
            console.print(f"[bold]Created:[/bold] {created_at}")

        # Show processing error if any
        error = item.get("processing_error")
        if error:
            console.print()
            console.print(f"[red][bold]Error:[/bold] {error}[/red]")

        # Show summary excerpt if available
        summary = item.get("extracted_summary")
        if summary:
            console.print()
            console.print("[bold]Summary:[/bold]")
            # Truncate long summaries
            if len(summary) > 500:
                console.print(f"[dim]{summary[:500]}...[/dim]")
            else:
                console.print(f"[dim]{summary}[/dim]")

    except Exception as e:
        console.print(f"[red]✗[/red] Show failed: {e}", style="bold red")
        raise typer.Exit(1)


@app.command()
def bind(
    ctx: typer.Context,
    data_id: str = typer.Argument(..., help="Data ID to bind"),
    scenario_id: Optional[str] = typer.Option(
        None,
        "--scenario",
        "-s",
        help="Scenario ID (defaults to current context)",
    ),
    role: Optional[str] = typer.Option(
        None,
        "--role",
        help="Data role (e.g., 'input', 'expected', 'ground_truth')",
    ),
    split: Optional[str] = typer.Option(
        None,
        "--split",
        help="Ground Truth split (train|dev|test). Allowed only with --role validation.",
    ),
    label_column: Optional[str] = typer.Option(
        None,
        "--label-column",
        help="Ground Truth label column. Allowed only with --role validation.",
    ),
    row_filter: Optional[str] = typer.Option(
        None,
        "--row-filter",
        help="Ground Truth row filter expression. Allowed only with --role validation.",
    ),
    sampling_seed: int = typer.Option(
        DEFAULT_GT_SAMPLING_SEED,
        "--sampling-seed",
        help="Ground Truth sampling seed (default: 42). Allowed only with --role validation.",
    ),
    materialize_gt: bool = typer.Option(
        True,
        "--materialize-gt/--no-materialize-gt",
        help="Materialize Ground Truth profile/contracts after validation bind.",
    ),
    api_url: Optional[str] = typer.Option(
        None,
        "--api-url",
        help="FluxLoop API base URL",
    ),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Minimal output"),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    Bind project data to a scenario.

    Creates an association between data in the project library and a scenario,
    allowing the scenario to use this data for testing.
    """
    role_value = _normalize_role(role)
    split_value = _normalize_split(split)
    sampling_seed_explicit = _was_option_explicitly_set(ctx, "sampling_seed")
    gt_options_requested = _gt_options_used(
        split=split_value,
        label_column=label_column,
        row_filter=row_filter,
        sampling_seed=sampling_seed,
        sampling_seed_explicit=sampling_seed_explicit,
    )

    if gt_options_requested and role_value != "validation":
        raise typer.BadParameter(
            "--split/--label-column/--row-filter/--sampling-seed require --role validation"
        )

    api_url = resolve_api_url(api_url, staging=staging)

    if not scenario_id:
        scenario_id = get_current_scenario_id()
        if not scenario_id:
            console.print("[yellow]No scenario selected.[/yellow]")
            console.print("[dim]Select one with: fluxloop scenarios select <id>[/dim]")
            raise typer.Exit(1)

    try:
        client = create_authenticated_client(api_url, use_jwt=True)

        bind_payload: Dict[str, Any] = {"data_id": data_id}

        if role_value:
            binding_meta: Dict[str, Any] = {"role": role_value}
            if role_value == "validation":
                binding_meta["sampling_seed"] = sampling_seed
                if split_value:
                    binding_meta["split"] = split_value
                if label_column:
                    binding_meta["label_column"] = label_column
                if row_filter:
                    binding_meta["row_filter"] = row_filter
            bind_payload["binding_meta"] = binding_meta

        if not quiet:
            console.print(f"[cyan]Binding data to scenario...[/cyan]")

        resp = client.post(
            f"/api/scenarios/{scenario_id}/data/bind",
            json=bind_payload,
        )

        already_bound = False
        if resp.status_code == 409:
            already_bound = True
            if not quiet:
                console.print("[yellow]⚠[/yellow] Data already bound to this scenario")
        else:
            handle_api_error(resp, "data binding")

        materialize_result: Optional[Dict[str, Any]] = None
        if role_value == "validation" and materialize_gt:
            materialize_result = _materialize_ground_truth(
                client=client,
                scenario_id=scenario_id,
                data_id=data_id,
                split=split_value,
                label_column=label_column,
                row_filter=row_filter,
                sampling_seed=sampling_seed,
            )

        if role_value == "validation":
            profile_id = _extract_profile_id(materialize_result or {})
            gt_contract_ids = _extract_gt_contract_ids(materialize_result or {})
            if quiet:
                console.print(f"data_id={data_id}")
                console.print(f"scenario_id={scenario_id}")
                console.print(f"profile_id={profile_id or '-'}")
                console.print(f"gt_contract_count={len(gt_contract_ids)}")
            else:
                console.print("[green]✓[/green] Validation (GT) binding complete")
                console.print(f"  data_id: {data_id}")
                console.print(f"  scenario_id: {scenario_id}")
                console.print(f"  profile_id: {profile_id or '-'}")
                console.print(f"  gt_contract_count: {len(gt_contract_ids)}")
        else:
            if not already_bound and not quiet:
                console.print("[green]✓[/green] Data bound to scenario")
            if quiet:
                console.print(f"data_id={data_id}")
                console.print(f"scenario_id={scenario_id}")
            else:
                console.print(f"  Data ID: {data_id}")
                console.print(f"  Scenario ID: {scenario_id}")

    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"[red]✗[/red] Binding failed: {e}", style="bold red")
        raise typer.Exit(1)


@gt_app.command("status")
def gt_status(
    scenario_id: str = typer.Option(..., "--scenario", help="Scenario ID"),
    data_id: Optional[str] = typer.Option(
        None,
        "--data-id",
        help="Filter by data ID",
    ),
    format: str = typer.Option(
        "table",
        "--format",
        help="Output format (table, json)",
    ),
    api_url: Optional[str] = typer.Option(
        None,
        "--api-url",
        help="FluxLoop API base URL",
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """Show Ground Truth materialization status for a scenario."""
    format_value = format.strip().lower()
    if format_value not in {"table", "json"}:
        raise typer.BadParameter("--format must be one of: table, json")

    api_url = resolve_api_url(api_url, staging=staging)

    try:
        client = create_authenticated_client(api_url, use_jwt=True)
        params = {"data_id": data_id} if data_id else None
        resp = client.get(
            f"/api/scenarios/{scenario_id}/ground-truth/status",
            params=params,
        )
        handle_api_error(resp, "ground truth status")

        rows = _build_gt_status_rows(resp.json(), fallback_data_id=data_id)

        if format_value == "json":
            console.print_json(json.dumps(rows, ensure_ascii=False, default=str))
            return

        if not rows:
            console.print("[yellow]No Ground Truth status found.[/yellow]")
            console.print("[dim]Bind validation data first: fluxloop data bind <data_id> --role validation[/dim]")
            return

        table = Table(title=f"Ground Truth Status ({scenario_id})")
        table.add_column("data_id", style="cyan")
        table.add_column("materialization_status", style="magenta")
        table.add_column("ground_truth_profile_id", style="green")
        table.add_column("gt_contract_count", justify="right")
        table.add_column("processing_status", style="yellow")
        table.add_column("updated_at", style="dim")

        for row in rows:
            table.add_row(
                str(row.get("data_id", "N/A")),
                str(row.get("materialization_status", "unknown")),
                str(row.get("ground_truth_profile_id", "-")),
                str(row.get("gt_contract_count", 0)),
                str(row.get("processing_status", "unknown")),
                str(row.get("updated_at", "-")),
            )

        console.print(table)

    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"[red]✗[/red] GT status failed: {e}", style="bold red")
        raise typer.Exit(1)


@app.command()
def unbind(
    binding_id: str = typer.Argument(..., help="Binding ID to remove"),
    scenario_id: Optional[str] = typer.Option(
        None,
        "--scenario",
        "-s",
        help="Scenario ID (defaults to current context)",
    ),
    api_url: Optional[str] = typer.Option(
        None,
        "--api-url",
        help="FluxLoop API base URL",
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    Remove a data binding from a scenario.
    """
    api_url = resolve_api_url(api_url, staging=staging)

    if not scenario_id:
        scenario_id = get_current_scenario_id()
        if not scenario_id:
            console.print("[yellow]No scenario selected.[/yellow]")
            console.print("[dim]Select one with: fluxloop scenarios select <id>[/dim]")
            raise typer.Exit(1)

    try:
        client = create_authenticated_client(api_url, use_jwt=True)

        console.print(f"[cyan]Removing data binding...[/cyan]")

        resp = client.delete(
            f"/api/scenarios/{scenario_id}/data/bind/{binding_id}",
        )

        handle_api_error(resp, "data unbinding")

        console.print(f"[green]✓[/green] Binding removed")

    except Exception as e:
        console.print(f"[red]✗[/red] Unbinding failed: {e}", style="bold red")
        raise typer.Exit(1)


@app.command()
def reprocess(
    data_id: str = typer.Argument(..., help="Data ID to reprocess"),
    as_type: Optional[str] = typer.Option(
        None,
        "--as",
        help="Change data category: 'document' or 'dataset'",
    ),
    project_id: Optional[str] = typer.Option(
        None,
        "--project-id",
        help="Project ID (defaults to current context)",
    ),
    api_url: Optional[str] = typer.Option(
        None,
        "--api-url",
        help="FluxLoop API base URL",
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    Reprocess data with a different category or to fix processing errors.

    Useful when:
    - Initial processing failed
    - You want to change the data category (document ↔ dataset)
    """
    api_url = resolve_api_url(api_url, staging=staging)

    if not project_id:
        project_id = get_current_web_project_id()
        if not project_id:
            console.print("[yellow]No Web Project selected.[/yellow]")
            console.print("[dim]Select one with: fluxloop projects select <id>[/dim]")
            raise typer.Exit(1)

    try:
        client = create_authenticated_client(api_url, use_jwt=True)

        reprocess_payload: Dict[str, Any] = {}

        if as_type:
            as_type_lower = as_type.lower()
            if as_type_lower == "dataset":
                reprocess_payload["data_category"] = "DATASET"
                reprocess_payload["processing_profile"] = "dataset"
            elif as_type_lower in {"document", "knowledge"}:
                reprocess_payload["data_category"] = "KNOWLEDGE"
                reprocess_payload["processing_profile"] = "document"

        console.print(f"[cyan]Reprocessing data...[/cyan]")

        resp = client.post(
            f"/api/projects/{project_id}/data/{data_id}/reprocess",
            json=reprocess_payload,
        )

        handle_api_error(resp, "data reprocess")

        result = resp.json()
        console.print(f"[green]✓[/green] Reprocessing queued")
        console.print(f"  Data ID: {data_id}")
        console.print(f"  Status: {result.get('processing_status', 'queued')}")

    except Exception as e:
        console.print(f"[red]✗[/red] Reprocess failed: {e}", style="bold red")
        raise typer.Exit(1)
