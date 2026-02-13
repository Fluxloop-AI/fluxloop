"""
Intent/Context refinement commands for FluxLoop CLI.
"""

from pathlib import Path
from typing import Any, Dict, Optional

import typer
from rich.console import Console

from ..api_utils import (
    handle_api_error,
    load_payload_file,
    resolve_api_url,
    save_cache_file,
)
from ..http_client import create_authenticated_client, post_with_retry
from ..context_manager import get_current_web_project_id

app = typer.Typer(help="Manage intent and context refinement")
console = Console()


@app.command()
def refine(
    intent: Optional[str] = typer.Option(
        None, "--intent", help="User intent description"
    ),
    project_id: Optional[str] = typer.Option(
        None, "--project-id", help="Project ID (defaults to current context)"
    ),
    scenario_id: Optional[str] = typer.Option(
        None, "--scenario-id", help="Associated scenario ID"
    ),
    apply: bool = typer.Option(
        True, "--apply/--no-apply", help="Apply refinements to project context"
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
    Refine intent and extract constraints using AI.
    
    Uses current project from context if --project-id is not specified.
    """
    api_url = resolve_api_url(api_url, staging=staging)

    # Use context if no project_id specified
    if not project_id:
        project_id = get_current_web_project_id()
        if not project_id:
            console.print("[yellow]No Web Project selected.[/yellow]")
            console.print("[dim]Select one with: fluxloop projects select <id>[/dim]")
            raise typer.Exit(1)

    # Build payload
    payload: Dict[str, Any] = {
        "project_id": project_id,
        "apply": apply,
    }

    if intent:
        payload["intent"] = intent

    if scenario_id:
        payload["scenario_id"] = scenario_id

    # Override with file if provided
    if file:
        file_data = load_payload_file(file)
        payload.update(file_data)

    # Validate required fields
    if "intent" not in payload:
        raise typer.BadParameter(
            "Either --intent or --file with 'intent' field is required"
        )

    try:
        console.print("[cyan]Refining intent and context...[/cyan]")

        client = create_authenticated_client(api_url, use_jwt=True)
        resp = post_with_retry(client, "/api/intents/refine", payload=payload)

        handle_api_error(resp, "context refinement")

        data = resp.json()

        # Display results
        console.print()
        console.print("[green]✓[/green] Intent extracted successfully")
        console.print("[green]✓[/green] Constraints identified")

        console.print("\n[bold]Result:[/bold]")

        if "intent" in data:
            console.print(f"  [bold cyan]Intent:[/bold cyan] {data['intent']}")

        if "constraints" in data and data["constraints"]:
            console.print(f"\n  [bold cyan]Constraints:[/bold cyan]")
            for constraint in data["constraints"]:
                console.print(f"    - {constraint}")

        if apply:
            console.print("\n[green]✓[/green] Saved to project context")

        # Save to cache
        cache_filename = f"{scenario_id}_context.yaml" if scenario_id else "latest_context.yaml"
        cache_path = save_cache_file("contexts", cache_filename, data)

        console.print(f"\n[dim]Saved to: {cache_path}[/dim]")

    except Exception as e:
        console.print(f"[red]✗[/red] Refinement failed: {e}", style="bold red")
        raise typer.Exit(1)
