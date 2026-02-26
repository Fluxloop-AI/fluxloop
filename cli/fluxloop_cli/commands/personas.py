"""
Persona management commands for FluxLoop CLI.
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import typer
from rich.console import Console
from rich.table import Table

from ..api_utils import (
    handle_api_error,
    load_payload_file,
    resolve_api_url,
    save_cache_file,
)
from ..http_client import create_authenticated_client, post_with_retry
from ..context_manager import get_current_web_project_id
from ..language import normalize_language_token
from ..progress import spin_while

app = typer.Typer(help="Manage test personas")
console = Console()

DEFAULT_PERSONAS_SUGGEST_TIMEOUT = 120.0


def _get_default_suggest_timeout() -> float:
    """Get suggest timeout from environment variable or use built-in default."""
    env_timeout = os.getenv("FLUXLOOP_PERSONAS_SUGGEST_TIMEOUT")
    if env_timeout:
        try:
            return float(env_timeout)
        except ValueError:
            pass
    return DEFAULT_PERSONAS_SUGGEST_TIMEOUT


@app.command()
def suggest(
    scenario_id: str = typer.Option(..., "--scenario-id", help="Scenario ID for persona suggestions"),
    project_id: Optional[str] = typer.Option(
        None, "--project-id", help="Project ID (defaults to current context)"
    ),
    count: int = typer.Option(3, "--count", help="Number of personas to suggest"),
    language: Optional[str] = typer.Option(
        None,
        "--language",
        help="Preferred language code for casting (e.g., ko, en, ja)",
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
    timeout_seconds: Optional[float] = typer.Option(
        None,
        "--timeout",
        help=(
            "Request timeout in seconds "
            "(default: 120, or FLUXLOOP_PERSONAS_SUGGEST_TIMEOUT env)"
        ),
    ),
):
    """
    Get AI-suggested personas for a scenario.
    
    Uses current project from context if --project-id is not specified.
    """
    api_url = resolve_api_url(api_url, staging=staging)
    effective_timeout = timeout_seconds or _get_default_suggest_timeout()

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
        "scenario_id": scenario_id,
        "count": count,
    }
    normalized_language = normalize_language_token(language)
    if normalized_language:
        payload["language"] = normalized_language

    # Override with file if provided
    if file:
        file_data = load_payload_file(file)
        payload.update(file_data)

    try:
        if effective_timeout > 60:
            console.print(f"[dim]Timeout: {effective_timeout:.0f}s[/dim]")

        client = create_authenticated_client(
            api_url,
            use_jwt=True,
            timeout=effective_timeout,
        )
        resp = spin_while(
            "Suggesting personas...",
            lambda: post_with_retry(client, "/api/personas/suggest", payload=payload),
            console=console,
        )

        handle_api_error(resp, f"persona suggestions for scenario {scenario_id}")

        data = resp.json()
        personas = data if isinstance(data, list) else data.get("personas", [])
        suggested_ids: List[str] = []
        if isinstance(data, dict):
            raw_ids = data.get("persona_ids")
            if isinstance(raw_ids, list):
                suggested_ids = [
                    pid for pid in raw_ids if isinstance(pid, str) and pid
                ]
        if not suggested_ids and isinstance(personas, list):
            suggested_ids = [
                persona.get("id")
                for persona in personas
                if isinstance(persona, dict) and isinstance(persona.get("id"), str)
            ]

        if not personas:
            console.print("[yellow]No personas suggested.[/yellow]")
            return

        console.print()
        console.print(f"[green]✓[/green] {len(personas)} personas suggested")

        # Create table
        console.print("\n[bold]Suggested personas:[/bold]")
        table = Table()
        table.add_column("Difficulty", style="cyan")
        table.add_column("Name", style="bold")
        table.add_column("Description")

        for persona in personas:
            attrs = persona.get("attributes") or {}
            difficulty = attrs.get("difficulty") or persona.get("difficulty", "unknown")
            difficulty_display = {
                "easy": "[green]Easy[/green]",
                "medium": "[yellow]Medium[/yellow]",
                "hard": "[red]Hard[/red]",
            }.get(difficulty, difficulty)
            description = (
                attrs.get("character_summary")
                or persona.get("description", "N/A")
            )

            table.add_row(
                difficulty_display,
                persona.get("name", "N/A"),
                description,
            )

        console.print(table)

        # Save to cache
        cache_path = save_cache_file(
            "personas",
            f"suggested_{scenario_id}.yaml",
            {"persona_ids": suggested_ids, "personas": personas},
        )
        console.print(f"\n[dim]Saved to: {cache_path}[/dim]")

        # Show next steps
        if suggested_ids:
            ids = ",".join(suggested_ids)
            console.print(
                f"\n[dim]Use in synthesis: fluxloop inputs synthesize --scenario-id {scenario_id} --persona-ids {ids}[/dim]"
            )

    except httpx.TimeoutException:
        console.print(
            f"[red]✗[/red] Persona suggestion timed out after {effective_timeout:.0f}s.\n"
            "  Options:\n"
            "    --timeout 180                        (increase timeout)\n"
            "    FLUXLOOP_PERSONAS_SUGGEST_TIMEOUT=180  (env default)",
            style="bold red",
        )
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]✗[/red] Suggestion failed: {e}", style="bold red")
        raise typer.Exit(1)


@app.command("list")
def list_personas(
    scenario_id: Optional[str] = typer.Option(
        None, "--scenario-id", help="Filter by scenario ID"
    ),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    List all personas.
    """
    api_url = resolve_api_url(api_url, staging=staging)

    try:
        client = create_authenticated_client(api_url, use_jwt=True)

        # Build query params
        params = {}
        if scenario_id:
            params["scenario_id"] = scenario_id

        resp = client.get("/api/personas", params=params)
        handle_api_error(resp, "personas list")

        data = resp.json()
        personas = data if isinstance(data, list) else data.get("personas", [])

        if not personas:
            console.print("[yellow]No personas found.[/yellow]")
            return

        # Create table
        table = Table(title="Personas")
        table.add_column("ID", style="cyan")
        table.add_column("Name", style="bold")
        table.add_column("Difficulty")
        table.add_column("Description")

        for persona in personas:
            attrs = persona.get("attributes") or {}
            difficulty = attrs.get("difficulty") or persona.get("difficulty", "unknown")
            difficulty_display = {
                "easy": "[green]Easy[/green]",
                "medium": "[yellow]Medium[/yellow]",
                "hard": "[red]Hard[/red]",
            }.get(difficulty, difficulty)
            description = (
                attrs.get("character_summary")
                or persona.get("description", "N/A")
            )

            table.add_row(
                persona.get("id", "N/A"),
                persona.get("name", "N/A"),
                difficulty_display,
                description[:50] + "..." if len(description) > 50 else description,
            )

        console.print(table)

    except Exception as e:
        console.print(f"[red]✗[/red] List failed: {e}", style="bold red")
        raise typer.Exit(1)


@app.command()
def create(
    name: str = typer.Option(..., "--name", help="Persona name"),
    description: Optional[str] = typer.Option(
        None, "--description", help="Persona description"
    ),
    file: Optional[Path] = typer.Option(
        None, "--file", "-f", help="Load full PersonaConfig from YAML or JSON file"
    ),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    Create a custom persona.
    """
    api_url = resolve_api_url(api_url, staging=staging)

    # Build payload
    payload: Dict[str, Any] = {"name": name}

    if description:
        payload["description"] = description

    # Override with file if provided
    if file:
        file_data = load_payload_file(file)
        payload.update(file_data)

    try:
        console.print("[cyan]Creating persona...[/cyan]")

        client = create_authenticated_client(api_url, use_jwt=True)
        resp = post_with_retry(client, "/api/personas", payload=payload)

        handle_api_error(resp, "persona creation")

        data = resp.json()

        console.print()
        console.print(
            f"[green]✓[/green] Persona created: [bold]{data.get('persona_id', 'N/A')}[/bold]"
        )
        console.print(f"  Name: {data.get('name', 'N/A')}")

        if "description" in data:
            console.print(f"  Description: {data['description']}")

    except Exception as e:
        console.print(f"[red]✗[/red] Creation failed: {e}", style="bold red")
        raise typer.Exit(1)
