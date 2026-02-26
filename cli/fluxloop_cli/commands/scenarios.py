"""
Scenario management commands for FluxLoop CLI.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ..api_utils import (
    handle_api_error,
    load_payload_file,
    resolve_api_url,
    save_cache_file,
)
from ..http_client import create_authenticated_client, post_with_retry
from ..language import normalize_language_token, DEFAULT_LANGUAGE
from ..constants import FLUXLOOP_DIR_NAME, SCENARIOS_DIR_NAME
from ..context_manager import (
    get_current_project_id,
    get_current_scenario_id,
    get_current_web_project_id,
    set_scenario,
)
from ..project_paths import find_workspace_root

app = typer.Typer(help="Manage test scenarios")
console = Console()


@app.command()
def refine(
    scenario_id: str = typer.Option(..., "--scenario-id", help="Scenario ID to refine"),
    project_id: Optional[str] = typer.Option(
        None, "--project-id", help="Project ID (defaults to current context)"
    ),
    apply: bool = typer.Option(
        True, "--apply/--no-apply", help="Apply refinements to scenario"
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
    Refine scenario goal and constraints using AI.
    
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
        "scenario_id": scenario_id,
        "apply": apply,
    }

    # Override with file if provided
    if file:
        file_data = load_payload_file(file)
        payload.update(file_data)

    try:
        console.print("[cyan]Refining scenario...[/cyan]")

        client = create_authenticated_client(api_url, use_jwt=True)
        resp = post_with_retry(client, "/api/scenarios/refine", payload=payload)

        handle_api_error(resp, f"scenario {scenario_id}")

        data = resp.json()

        # Display results
        console.print()
        console.print("[green]✓[/green] Scenario refinement complete")

        if "goal" in data:
            console.print(f"\n[bold]Goal:[/bold] {data['goal']}")

        if "constraints" in data and data["constraints"]:
            console.print(f"\n[bold]Constraints:[/bold]")
            for constraint in data["constraints"]:
                console.print(f"  - {constraint}")

        # Save to cache
        cache_path = save_cache_file(
            "scenarios", f"{scenario_id}.yaml", data
        )
        console.print(f"\n[dim]Saved to: {cache_path}[/dim]")

    except Exception as e:
        console.print(f"[red]✗[/red] Refinement failed: {e}", style="bold red")
        raise typer.Exit(1)


def _fetch_project_default_language(api_url: str, project_id: str) -> str:
    """Fetch project's default_language from API, falling back to DEFAULT_LANGUAGE."""
    try:
        client = create_authenticated_client(api_url, use_jwt=True)
        resp = client.get(f"/api/projects/{project_id}")
        if resp.status_code == 200:
            settings = resp.json().get("settings") or {}
            token = normalize_language_token(settings.get("default_language"))
            return token or DEFAULT_LANGUAGE
    except Exception:
        pass
    return DEFAULT_LANGUAGE


@app.command()
def create(
    name: str = typer.Option(..., "--name", help="Scenario name"),
    project_id: Optional[str] = typer.Option(
        None, "--project-id", help="Project ID (defaults to current context)"
    ),
    description: Optional[str] = typer.Option(
        None, "--description", help="Scenario description"
    ),
    goal: Optional[str] = typer.Option(
        None, "--goal", help="Scenario goal (one sentence objective)"
    ),
    constraint: Optional[List[str]] = typer.Option(
        None, "--constraint", help="Constraint (can be specified multiple times)"
    ),
    assumption: Optional[List[str]] = typer.Option(
        None, "--assumption", help="Assumption/prerequisite (can be specified multiple times)"
    ),
    success_criteria: Optional[List[str]] = typer.Option(
        None, "--success-criteria", help="Success criteria (can be specified multiple times)"
    ),
    language: Optional[str] = typer.Option(
        None, "--language",
        help="Language code for scenario (e.g., ko, en, ja). Defaults to project setting.",
    ),
    config_file: Optional[Path] = typer.Option(
        None, "--config-file", help="Path to config file for snapshot (overrides inline options)"
    ),
    file: Optional[Path] = typer.Option(
        None, "--file", "-f", help="Load full payload from YAML or JSON file"
    ),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
    select: bool = typer.Option(
        True, "--select/--no-select", help="Automatically select the created scenario"
    ),
):
    """
    Create a new test scenario.
    
    Uses current project from context if --project-id is not specified.
    
    Config snapshot can be provided via:
    1. Inline options: --goal, --constraint, --assumption, --success-criteria
    2. Config file: --config-file <path>
    3. Full payload file: --file <path>
    
    Priority: --file > --config-file > inline options > default (goal=name)
    
    Examples:
        # Minimal (uses name as goal)
        fluxloop scenarios create --name "SQL Query Test"
        
        # With inline options
        fluxloop scenarios create --name "SQL Query Test" \\
            --goal "Test SQL query execution and result interpretation" \\
            --constraint "Response must be in Korean" \\
            --constraint "Response time under 30 seconds" \\
            --assumption "User has no SQL knowledge"
    """
    api_url = resolve_api_url(api_url, staging=staging)
    
    # Use context if no project_id specified
    if not project_id:
        project_id = get_current_project_id()
        if not project_id:
            console.print("[yellow]No project selected.[/yellow]")
            console.print("[dim]Select one with: fluxloop projects select <id>[/dim]")
            raise typer.Exit(1)

    # Build config_snapshot from inline options (lowest priority)
    config_snapshot: Dict[str, Any] = {}
    
    # Use name as default goal if no goal specified
    effective_goal = goal if goal else name
    config_snapshot["goal"] = effective_goal
    
    if constraint:
        config_snapshot["constraints"] = constraint
    
    if assumption:
        config_snapshot["assumptions"] = assumption
    
    if success_criteria:
        config_snapshot["success_criteria"] = success_criteria

    # Override with config_file if provided (medium priority)
    if config_file:
        if not config_file.exists():
            raise typer.BadParameter(f"Config file not found: {config_file}")
        config_snapshot = load_payload_file(config_file)

    # Build payload
    payload: Dict[str, Any] = {
        "name": name,
        "project_id": project_id,
        "config_snapshot": config_snapshot,
    }

    if description:
        payload["description"] = description

    # Override with file if provided (highest priority)
    if file:
        file_data = load_payload_file(file)
        payload.update(file_data)

    # Language resolution runs AFTER --file merge so the final
    # config_snapshot and project_id are authoritative.
    # Priority: config/file language > --language > project default > "en"
    final_snapshot: Dict[str, Any] = payload.get("config_snapshot") or {}
    existing_lang = normalize_language_token(final_snapshot.get("language"))
    if existing_lang:
        final_snapshot["language"] = existing_lang
    else:
        explicit_lang = normalize_language_token(language)
        if explicit_lang:
            final_snapshot["language"] = explicit_lang
        else:
            final_project_id = payload.get("project_id", project_id)
            final_snapshot["language"] = _fetch_project_default_language(
                api_url, final_project_id
            )
    payload["config_snapshot"] = final_snapshot

    try:
        console.print("[cyan]Creating scenario...[/cyan]")

        client = create_authenticated_client(api_url, use_jwt=True)
        resp = post_with_retry(client, "/api/scenarios", payload=payload)

        handle_api_error(resp, "scenario creation")

        data = resp.json()
        scenario_id = data.get('scenario_id', data.get('id', 'N/A'))
        scenario_name = data.get('name', name)

        console.print()
        console.print(
            f"[green]✓[/green] Scenario created: [bold]{scenario_id}[/bold]"
        )
        console.print(f"  Name: {scenario_name}")

        if "description" in data:
            console.print(f"  Description: {data['description']}")
        
        # Automatically select if requested
        if select and scenario_id != 'N/A':
            set_scenario(scenario_id, scenario_name)
            console.print(f"[green]✓[/green] Scenario selected as current context")

    except Exception as e:
        console.print(f"[red]✗[/red] Creation failed: {e}", style="bold red")
        raise typer.Exit(1)


@app.command()
def generate(
    intent: Optional[str] = typer.Option(
        None, "--intent", help="User intent description"
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
    Generate scenario using Alignment Agent from user intent.
    """
    api_url = resolve_api_url(api_url, staging=staging)

    # Build payload
    payload: Dict[str, Any] = {}

    if intent:
        payload["intent"] = intent

    # Override with file if provided
    if file:
        file_data = load_payload_file(file)
        payload.update(file_data)

    if "intent" not in payload:
        raise typer.BadParameter("Either --intent or --file with 'intent' field is required")

    try:
        console.print("[cyan]Generating scenario from intent...[/cyan]")

        client = create_authenticated_client(api_url, use_jwt=True)
        resp = post_with_retry(client, "/api/scenarios/generate", payload=payload)

        handle_api_error(resp, "scenario generation")

        data = resp.json()

        console.print()
        console.print("[green]✓[/green] Scenario generated")

        if "scenario_id" in data:
            console.print(f"  Scenario ID: [bold]{data['scenario_id']}[/bold]")

        if "name" in data:
            console.print(f"  Name: {data['name']}")

        if "goal" in data:
            console.print(f"  Goal: {data['goal']}")

        if "constraints" in data and data["constraints"]:
            console.print(f"\n  Constraints:")
            for constraint in data["constraints"]:
                console.print(f"    - {constraint}")

    except Exception as e:
        console.print(f"[red]✗[/red] Generation failed: {e}", style="bold red")
        raise typer.Exit(1)


@app.command("list")
def list_scenarios(
    project_id: Optional[str] = typer.Option(
        None, "--project-id", help="Filter by project ID (defaults to current context)"
    ),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    List all scenarios for a project.
    
    Uses current project from context if --project-id is not specified.
    """
    api_url = resolve_api_url(api_url, staging=staging)
    
    # Use context if no project_id specified
    if not project_id:
        project_id = get_current_project_id()
        if not project_id:
            console.print("[yellow]No project selected.[/yellow]")
            console.print("[dim]Select one with: fluxloop projects select <id>[/dim]")
            raise typer.Exit(1)

    try:
        client = create_authenticated_client(api_url, use_jwt=True)

        # Build query params
        params = {"project_id": project_id}

        resp = client.get("/api/scenarios", params=params)
        handle_api_error(resp, "scenarios list")

        data = resp.json()
        scenarios = data if isinstance(data, list) else data.get("scenarios", [])
        
        # Get current scenario to mark selected
        current_scenario_id = get_current_scenario_id()

        if not scenarios:
            console.print("[yellow]No scenarios found.[/yellow]")
            console.print("[dim]Create one with: fluxloop scenarios create --name <name>[/dim]")
            return

        # Create table
        table = Table(title=f"Scenarios (Project: {project_id})")
        table.add_column("ID", style="cyan")
        table.add_column("Name", style="bold")
        table.add_column("Created", style="dim")
        table.add_column("Selected", style="green")

        for scenario in scenarios:
            scenario_id = scenario.get("id", scenario.get("scenario_id", "N/A"))
            is_selected = "✓" if scenario_id == current_scenario_id else ""
            table.add_row(
                scenario_id,
                scenario.get("name", "N/A"),
                scenario.get("created_at", "N/A"),
                is_selected,
            )

        console.print(table)
        console.print()
        console.print("[dim]Select a scenario: fluxloop scenarios select <id>[/dim]")

    except Exception as e:
        console.print(f"[red]✗[/red] List failed: {e}", style="bold red")
        raise typer.Exit(1)


def _find_local_scenarios() -> List[str]:
    """Find existing local scenario folders in .fluxloop/scenarios/."""
    workspace_root = find_workspace_root()
    if not workspace_root:
        return []
    scenarios_dir = workspace_root / FLUXLOOP_DIR_NAME / SCENARIOS_DIR_NAME
    if not scenarios_dir.exists():
        return []
    return [
        d.name for d in scenarios_dir.iterdir() 
        if d.is_dir() and not d.name.startswith(".")
    ]


@app.command()
def select(
    scenario_id: str = typer.Argument(..., help="Scenario ID to select"),
    local_path: Optional[str] = typer.Option(
        None, "--local-path", "-l", help="Local scenario folder name (in .fluxloop/scenarios/)"
    ),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    Select a scenario as the current working scenario.
    
    Use --local-path to specify which local folder to link.
    If not specified, tries to find existing folders automatically.
    """
    api_url = resolve_api_url(api_url, staging=staging)

    try:
        # Fetch scenario details to get the name
        client = create_authenticated_client(api_url, use_jwt=True)
        resp = client.get(f"/api/scenarios/{scenario_id}")
        handle_api_error(resp, "scenario fetch")
        data = resp.json()

        scenario_name = data.get("name", "Unknown")
        
        # Determine local_path
        effective_local_path = None
        if local_path:
            # User explicitly specified
            effective_local_path = f"{SCENARIOS_DIR_NAME}/{local_path}"
            console.print(f"[dim]Using local folder: {local_path}[/dim]")
        else:
            # Try to find existing folders
            existing_folders = _find_local_scenarios()
            if len(existing_folders) == 1:
                # Only one folder exists, use it
                effective_local_path = f"{SCENARIOS_DIR_NAME}/{existing_folders[0]}"
                console.print(f"[dim]Auto-detected local folder: {existing_folders[0]}[/dim]")
            elif len(existing_folders) > 1:
                # Multiple folders, show warning
                console.print(f"[yellow]⚠[/yellow] Multiple local folders found: {', '.join(existing_folders)}")
                console.print(f"[dim]Using scenario name '{scenario_name}' as local path.[/dim]")
                console.print(f"[dim]Use --local-path to specify a different folder.[/dim]")
        
        set_scenario(scenario_id, scenario_name, effective_local_path)

        console.print(f"[green]✓[/green] Scenario selected: [bold]{scenario_name}[/bold]")
        console.print(f"  ID: {scenario_id}")
        if effective_local_path:
            console.print(f"  Local: {effective_local_path}")

    except Exception as e:
        console.print(f"[red]✗[/red] Selection failed: {e}", style="bold red")
        raise typer.Exit(1)


@app.command()
def show(
    scenario_id: str = typer.Option(..., "--scenario-id", help="Scenario ID to show"),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    Show scenario details.
    """
    api_url = resolve_api_url(api_url, staging=staging)

    try:
        client = create_authenticated_client(api_url, use_jwt=True)
        resp = client.get(f"/api/scenarios/{scenario_id}")

        handle_api_error(resp, f"scenario {scenario_id}")

        data = resp.json()

        # Build panel content
        content_lines = []

        if "name" in data:
            content_lines.append(f"[bold]Name:[/bold] {data['name']}")

        if "goal" in data:
            content_lines.append(f"\n[bold]Goal:[/bold]\n{data['goal']}")

        if "constraints" in data and data["constraints"]:
            content_lines.append(f"\n[bold]Constraints:[/bold]")
            for constraint in data["constraints"]:
                content_lines.append(f"  - {constraint}")

        if "personas" in data and data["personas"]:
            content_lines.append(f"\n[bold]Personas:[/bold] {len(data['personas'])} configured")

        if "created_at" in data:
            content_lines.append(f"\n[dim]Created: {data['created_at']}[/dim]")

        panel = Panel(
            "\n".join(content_lines),
            title=f"[bold blue]Scenario: {scenario_id}[/bold blue]",
            border_style="blue",
        )

        console.print(panel)

    except Exception as e:
        console.print(f"[red]✗[/red] Show failed: {e}", style="bold red")
        raise typer.Exit(1)


@app.command()
def update(
    scenario_id: str = typer.Option(..., "--scenario-id", help="Scenario ID to update"),
    file: Path = typer.Option(..., "--file", "-f", help="Load update payload from YAML or JSON file"),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    Update scenario fields.
    """
    api_url = resolve_api_url(api_url, staging=staging)

    # Load payload from file
    payload = load_payload_file(file)

    try:
        console.print(f"[cyan]Updating scenario {scenario_id}...[/cyan]")

        client = create_authenticated_client(api_url, use_jwt=True)
        resp = client.patch(f"/api/scenarios/{scenario_id}", json=payload)

        handle_api_error(resp, f"scenario {scenario_id}")

        data = resp.json()

        console.print()
        console.print(f"[green]✓[/green] Scenario updated: [bold]{scenario_id}[/bold]")

        if "name" in data:
            console.print(f"  Name: {data['name']}")

    except Exception as e:
        console.print(f"[red]✗[/red] Update failed: {e}", style="bold red")
        raise typer.Exit(1)
