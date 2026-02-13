"""
Bundle management commands for FluxLoop CLI.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ..api_utils import (
    handle_api_error,
    load_payload_file,
    resolve_api_url,
)
from ..context_manager import get_current_web_project_id
from ..http_client import create_authenticated_client, post_with_retry

app = typer.Typer(help="Manage test bundles")
console = Console()


def _publish_bundle(client: Any, bundle_version_id: str):
    """Publish a bundle via the dedicated publish endpoint."""
    return client.post(f"/api/bundles/{bundle_version_id}/publish")


@app.command()
def publish(
    scenario_id: str = typer.Option(..., "--scenario-id", help="Scenario ID for the bundle"),
    input_set_id: str = typer.Option(..., "--input-set-id", help="Input set ID to include"),
    project_id: Optional[str] = typer.Option(
        None, "--project-id", help="Project ID (auto-detected from context)"
    ),
    name: Optional[str] = typer.Option(
        None, "--name", help="Bundle name"
    ),
    file: Optional[Path] = typer.Option(
        None, "--file", "-f", help="Load payload from YAML or JSON file"
    ),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    Create and publish a bundle (combined operation).

    This command performs two operations:
    1. Creates a bundle with status 'draft'
    2. Publishes the bundle via publish endpoint
    """
    api_url = resolve_api_url(api_url, staging=staging)

    # Use context if no project_id specified
    if not project_id:
        project_id = get_current_web_project_id()
        if not project_id:
            console.print("[yellow]No Web Project selected.[/yellow]")
            console.print("[dim]Select one with: fluxloop projects select <id>[/dim]")
            raise typer.Exit(1)

    # Auto-generate name if not provided
    if not name:
        name = f"bundle-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    # Build payload
    payload: Dict[str, Any] = {
        "project_id": project_id,
        "scenario_id": scenario_id,
        "input_set_id": input_set_id,
        "name": name,
        "status": "draft",
    }

    # Override with file if provided
    if file:
        file_data = load_payload_file(file)
        payload.update(file_data)

    try:
        console.print("[cyan]Publishing bundle...[/cyan]")

        client = create_authenticated_client(api_url, use_jwt=True)

        # Step 1: Create bundle
        console.print("  [dim]Creating bundle (draft)...[/dim]")
        resp = post_with_retry(client, "/api/bundles", payload=payload)
        handle_api_error(resp, "bundle creation")

        data = resp.json()
        # Support both 'id' and 'bundle_version_id' field names
        bundle_version_id = data.get("bundle_version_id") or data.get("id")

        if not bundle_version_id:
            raise ValueError("Bundle creation succeeded but no bundle_version_id returned")

        console.print(f"  [green]✓[/green] Bundle created: {bundle_version_id} (draft)")

        # Display bundle info
        console.print(f"  [green]✓[/green] Scenario: {scenario_id}")
        console.print(f"  [green]✓[/green] Input set: {input_set_id} ({data.get('input_count', 'N/A')} inputs)")

        if "personas_count" in data:
            console.print(f"  [green]✓[/green] Personas: {data['personas_count']}")

        # Step 2: Publish bundle using dedicated endpoint
        console.print("  [dim]Publishing bundle...[/dim]")
        resp = _publish_bundle(client, bundle_version_id)
        handle_api_error(resp, f"bundle {bundle_version_id} publish")

        console.print(f"  [green]✓[/green] Status changed: draft → published")

        # Success
        console.print()
        console.print("[bold]Bundle published successfully:[/bold]")
        console.print(f"  Bundle Version ID: [bold cyan]{bundle_version_id}[/bold cyan]")
        console.print(f"  Status: [bold green]published[/bold green]")

        # Next steps
        console.print(
            f"\n[dim]Run tests: fluxloop test --bundle-version-id {bundle_version_id}[/dim]"
        )

    except ValueError as e:
        console.print(f"\n[red]✗[/red] {e}", style="bold red")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"\n[red]✗[/red] Publication failed: {e}", style="bold red")

        # If we created a bundle but failed to publish, provide recovery instructions
        if "bundle_version_id" in locals():
            console.print(f"\n[yellow]Created bundle (draft): {bundle_version_id}[/yellow]")
            console.print("\n[bold]Recovery options:[/bold]")
            console.print(f"  Publish now:   fluxloop bundles update --bundle-version-id {bundle_version_id} --status published")
            console.print(f"  Or delete:     fluxloop bundles delete --bundle-version-id {bundle_version_id}")

        raise typer.Exit(1)


@app.command()
def create(
    scenario_id: str = typer.Option(..., "--scenario-id", help="Scenario ID for the bundle"),
    input_set_id: str = typer.Option(..., "--input-set-id", help="Input set ID to include"),
    project_id: Optional[str] = typer.Option(
        None, "--project-id", help="Project ID (auto-detected from context)"
    ),
    name: Optional[str] = typer.Option(
        None, "--name", help="Bundle name"
    ),
    file: Optional[Path] = typer.Option(
        None, "--file", "-f", help="Load payload from YAML or JSON file"
    ),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    Create a new bundle (draft status).
    """
    api_url = resolve_api_url(api_url, staging=staging)

    # Use context if no project_id specified
    if not project_id:
        project_id = get_current_web_project_id()
        if not project_id:
            console.print("[yellow]No Web Project selected.[/yellow]")
            console.print("[dim]Select one with: fluxloop projects select <id>[/dim]")
            raise typer.Exit(1)

    # Auto-generate name if not provided
    if not name:
        name = f"bundle-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    # Build payload
    payload: Dict[str, Any] = {
        "project_id": project_id,
        "scenario_id": scenario_id,
        "input_set_id": input_set_id,
        "name": name,
        "status": "draft",
    }

    # Override with file if provided
    if file:
        file_data = load_payload_file(file)
        payload.update(file_data)

    try:
        console.print("[cyan]Creating bundle...[/cyan]")

        client = create_authenticated_client(api_url, use_jwt=True)
        resp = post_with_retry(client, "/api/bundles", payload=payload)

        handle_api_error(resp, "bundle creation")

        data = resp.json()

        # Support both 'id' and 'bundle_version_id' field names
        bundle_id = data.get("bundle_version_id") or data.get("id", "N/A")

        console.print()
        console.print(
            f"[green]✓[/green] Bundle created: [bold]{bundle_id}[/bold]"
        )
        console.print(f"  Status: {data.get('status', 'draft')}")

        if "name" in data:
            console.print(f"  Name: {data['name']}")

    except Exception as e:
        console.print(f"[red]✗[/red] Creation failed: {e}", style="bold red")
        raise typer.Exit(1)


@app.command("list")
def list_bundles(
    scenario_id: Optional[str] = typer.Option(
        None, "--scenario-id", help="Filter by scenario ID"
    ),
    project_id: Optional[str] = typer.Option(
        None, "--project-id", help="Filter by project ID"
    ),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    List all bundles.
    """
    api_url = resolve_api_url(api_url, staging=staging)

    # Use context if no project_id specified
    if not project_id:
        project_id = get_current_web_project_id()
        if not project_id:
            console.print("[yellow]No Web Project selected.[/yellow]")
            console.print("[dim]Select one with: fluxloop projects select <id>[/dim]")
            raise typer.Exit(1)

    try:
        client = create_authenticated_client(api_url, use_jwt=True)

        # Build query params
        params = {"project_id": project_id}
        if scenario_id:
            params["scenario_id"] = scenario_id

        resp = client.get("/api/bundles", params=params)
        handle_api_error(resp, "bundles list")

        data = resp.json()
        bundles = data if isinstance(data, list) else data.get("bundles", [])

        if not bundles:
            console.print("[yellow]No bundles found.[/yellow]")
            if scenario_id:
                console.print(
                    f"[dim]Create one: fluxloop bundles publish --scenario-id {scenario_id} --input-set-id <input_set_id>[/dim]"
                )
            else:
                console.print(
                    "[dim]Create one: fluxloop bundles publish --scenario-id <id> --input-set-id <id>[/dim]"
                )
            return

        # Create table
        table = Table(title="Bundles")
        table.add_column("ID", style="cyan")
        table.add_column("Name", style="bold")
        table.add_column("Status", style="green")
        table.add_column("Created", style="dim")

        for bundle in bundles:
            status = bundle.get("status", "unknown")
            status_display = {
                "draft": "[yellow]draft[/yellow]",
                "published": "[green]published[/green]",
                "archived": "[dim]archived[/dim]",
            }.get(status, status)

            table.add_row(
                bundle.get("bundle_version_id", bundle.get("id", "N/A")),
                bundle.get("name", "N/A"),
                status_display,
                bundle.get("created_at", "N/A"),
            )

        console.print(table)

    except Exception as e:
        console.print(f"[red]✗[/red] List failed: {e}", style="bold red")
        raise typer.Exit(1)


@app.command()
def show(
    bundle_version_id: str = typer.Option(..., "--bundle-version-id", help="Bundle version ID to show"),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    Show bundle details.
    """
    api_url = resolve_api_url(api_url, staging=staging)

    try:
        client = create_authenticated_client(api_url, use_jwt=True)
        resp = client.get(f"/api/bundles/{bundle_version_id}")

        handle_api_error(resp, f"bundle {bundle_version_id}")

        data = resp.json()

        # Build panel content
        content_lines = []

        if "name" in data:
            content_lines.append(f"[bold]Name:[/bold] {data['name']}")

        if "status" in data:
            status = data["status"]
            status_display = {
                "draft": "[yellow]draft[/yellow]",
                "published": "[green]published[/green]",
                "archived": "[dim]archived[/dim]",
            }.get(status, status)
            content_lines.append(f"[bold]Status:[/bold] {status_display}")

        if "scenario_id" in data:
            content_lines.append(f"\n[bold]Scenario:[/bold] {data['scenario_id']}")

        if "input_set_id" in data:
            content_lines.append(f"[bold]Input Set:[/bold] {data['input_set_id']}")

        if "input_count" in data:
            content_lines.append(f"[bold]Input Count:[/bold] {data['input_count']}")

        if "personas_count" in data:
            content_lines.append(f"[bold]Personas:[/bold] {data['personas_count']}")

        if "created_at" in data:
            content_lines.append(f"\n[dim]Created: {data['created_at']}[/dim]")

        panel = Panel(
            "\n".join(content_lines),
            title=f"[bold blue]Bundle: {bundle_version_id}[/bold blue]",
            border_style="blue",
        )

        console.print(panel)

    except Exception as e:
        console.print(f"[red]✗[/red] Show failed: {e}", style="bold red")
        raise typer.Exit(1)


@app.command()
def update(
    bundle_version_id: str = typer.Option(..., "--bundle-version-id", help="Bundle version ID to update"),
    status: Optional[str] = typer.Option(
        None, "--status", help="New status (draft, published, archived)"
    ),
    file: Optional[Path] = typer.Option(
        None, "--file", "-f", help="Load update payload from YAML or JSON file"
    ),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    Update bundle fields.
    """
    api_url = resolve_api_url(api_url, staging=staging)

    # Build payload
    payload: Dict[str, Any] = {}

    if status:
        payload["status"] = status

    # Override with file if provided
    if file:
        file_data = load_payload_file(file)
        payload.update(file_data)

    if not payload:
        raise typer.BadParameter("No updates provided. Use --status or --file")

    try:
        console.print(f"[cyan]Updating bundle {bundle_version_id}...[/cyan]")

        client = create_authenticated_client(api_url, use_jwt=True)
        if payload.get("status") == "published":
            if len(payload) > 1:
                raise typer.BadParameter(
                    "Publishing with --status published cannot be combined with other updates. "
                    "Use a separate update command for non-status fields."
                )
            resp = _publish_bundle(client, bundle_version_id)
        else:
            resp = client.put(f"/api/bundles/{bundle_version_id}", json=payload)

        handle_api_error(resp, f"bundle {bundle_version_id}")

        data = resp.json()

        console.print()
        console.print(f"[green]✓[/green] Bundle updated: [bold]{bundle_version_id}[/bold]")

        if "status" in data:
            console.print(f"  Status: {data['status']}")

    except Exception as e:
        console.print(f"[red]✗[/red] Update failed: {e}", style="bold red")
        raise typer.Exit(1)
