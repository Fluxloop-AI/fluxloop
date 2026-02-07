"""
Project data management commands for FluxLoop CLI.

Provides commands for uploading, listing, and managing project data (Knowledge/Dataset).
Implements the data push → confirm → (optional) bind workflow.
"""

from __future__ import annotations

import hashlib
import mimetypes
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import typer
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
console = Console()


# Extension-based auto-detection for data category
DATASET_EXTENSIONS = {".csv", ".json", ".jsonl", ".xlsx", ".xls", ".tsv"}
DOCUMENT_EXTENSIONS = {".pdf", ".docx", ".doc", ".md", ".txt", ".html", ".htm"}


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


def _infer_file_type(file_path: Path) -> str:
    """Infer file type from extension."""
    ext = file_path.suffix.lower().lstrip(".")
    return ext or "unknown"


@app.command()
def push(
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
    # Validate file exists
    file = file.expanduser().resolve()
    if not file.exists():
        console.print(f"[red]✗[/red] File not found: {file}")
        raise typer.Exit(1)
    if not file.is_file():
        console.print(f"[red]✗[/red] Not a file: {file}")
        raise typer.Exit(1)

    # Resolve project
    api_url = resolve_api_url(api_url)
    if not project_id:
        project_id = get_current_web_project_id()
        if not project_id:
            console.print("[yellow]No Web Project selected.[/yellow]")
            console.print("[dim]Select one with: fluxloop projects select <id>[/dim]")
            raise typer.Exit(1)

    # Infer data category
    data_category = _infer_data_category(file, as_type)
    category_display = "Dataset" if data_category == "DATASET" else "Document"

    # Get file metadata
    filename = file.name
    file_size = file.stat().st_size
    mime_type = _infer_mime_type(file)
    file_type = _infer_file_type(file)

    if not quiet:
        console.print(f"[cyan]Uploading {filename}...[/cyan]")
        console.print(f"  Type: {category_display} ({data_category})")
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
            "processing_profile": "auto",
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
        scenario_id = scenario
        if bind and not scenario_id:
            scenario_id = get_current_scenario_id()

        if scenario_id:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
                transient=True,
            ) as progress:
                task = progress.add_task("Binding to scenario...", total=None)

                bind_payload = {"data_id": data_id}
                bind_resp = client.post(
                    f"/api/scenarios/{scenario_id}/data/bind",
                    json=bind_payload,
                )

                if bind_resp.status_code == 404:
                    console.print(f"[yellow]⚠[/yellow] Scenario not found: {scenario_id}")
                elif bind_resp.status_code == 409:
                    if not quiet:
                        console.print(f"[dim]Already bound to scenario[/dim]")
                elif bind_resp.is_success:
                    if not quiet:
                        console.print(f"[green]✓[/green] Bound to scenario: {scenario_id}")
                else:
                    console.print(f"[yellow]⚠[/yellow] Binding failed: {bind_resp.status_code}")

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
    api_url: Optional[str] = typer.Option(
        None,
        "--api-url",
        help="FluxLoop API base URL",
    ),
):
    """
    List all data in the project library.

    Shows both KNOWLEDGE (documents) and DATASET entries with their processing status.
    """
    api_url = resolve_api_url(api_url)

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
):
    """
    Show details of a specific data record.
    """
    api_url = resolve_api_url(api_url)

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
    api_url: Optional[str] = typer.Option(
        None,
        "--api-url",
        help="FluxLoop API base URL",
    ),
):
    """
    Bind project data to a scenario.

    Creates an association between data in the project library and a scenario,
    allowing the scenario to use this data for testing.
    """
    api_url = resolve_api_url(api_url)

    if not scenario_id:
        scenario_id = get_current_scenario_id()
        if not scenario_id:
            console.print("[yellow]No scenario selected.[/yellow]")
            console.print("[dim]Select one with: fluxloop scenarios select <id>[/dim]")
            raise typer.Exit(1)

    try:
        client = create_authenticated_client(api_url, use_jwt=True)

        bind_payload: Dict[str, Any] = {"data_id": data_id}

        if role:
            bind_payload["binding_meta"] = {"role": role}

        console.print(f"[cyan]Binding data to scenario...[/cyan]")

        resp = client.post(
            f"/api/scenarios/{scenario_id}/data/bind",
            json=bind_payload,
        )

        if resp.status_code == 409:
            console.print(f"[yellow]⚠[/yellow] Data already bound to this scenario")
            return

        handle_api_error(resp, "data binding")

        console.print(f"[green]✓[/green] Data bound to scenario")
        console.print(f"  Data ID: {data_id}")
        console.print(f"  Scenario ID: {scenario_id}")

    except Exception as e:
        console.print(f"[red]✗[/red] Binding failed: {e}", style="bold red")
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
):
    """
    Remove a data binding from a scenario.
    """
    api_url = resolve_api_url(api_url)

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
):
    """
    Reprocess data with a different category or to fix processing errors.

    Useful when:
    - Initial processing failed
    - You want to change the data category (document ↔ dataset)
    """
    api_url = resolve_api_url(api_url)

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
