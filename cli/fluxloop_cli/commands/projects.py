"""
Project management commands for FluxLoop CLI.

Architecture:
- Web Project ↔ .fluxloop/ workspace
- 'projects select' creates .fluxloop/project.json to link workspace to Web Project
"""

from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from ..http_client import create_authenticated_client, post_with_retry
from ..api_utils import handle_api_error, resolve_api_url
from ..language import normalize_language_token
from ..context_manager import (
    select_web_project,
    load_project_connection,
    get_current_web_project_id,
)

app = typer.Typer(help="Manage FluxLoop projects")
console = Console()


def _resolve_workspace_id(
    client, workspace_id: Optional[str]
) -> str:
    if workspace_id:
        return workspace_id

    resp = client.get("/api/workspaces")
    handle_api_error(resp, "list workspaces")
    data = resp.json()
    workspaces = data if isinstance(data, list) else data.get("workspaces", [])

    if not workspaces:
        raise typer.BadParameter("No workspaces found. Create one in the web app first.")

    if len(workspaces) == 1:
        return workspaces[0].get("id")

    console.print("[yellow]Multiple workspaces detected.[/yellow]")
    for workspace in workspaces:
        console.print(
            f"  - {workspace.get('id', '')}: {workspace.get('name', '')}"
        )
    raise typer.BadParameter("Please specify --workspace-id.")


@app.command("list")
def list_projects(
    format: str = typer.Option(
        "table", "--format", help="Output format (table, json)"
    ),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    List all Web Projects you have access to.
    
    Shows which project is currently selected for this workspace.
    """
    api_url = resolve_api_url(api_url, staging=staging)
    
    try:
        client = create_authenticated_client(api_url, use_jwt=True)
        resp = client.get("/api/projects")
        handle_api_error(resp, "list projects")
        
        data = resp.json()
        projects = data.get("projects", data) if isinstance(data, dict) else data
        
        if not projects:
            console.print("[yellow]No projects found.[/yellow]")
            console.print("[dim]Create one with: fluxloop projects create --name <name>[/dim]")
            return

        if format == "json":
            import json

            console.print_json(json.dumps(projects, ensure_ascii=False, default=str))
            return

        # Get current workspace project
        workspace_project_id = get_current_web_project_id()
        
        # Display as table
        table = Table(title="Web Projects")
        table.add_column("ID", style="dim")
        table.add_column("Name", style="bold")
        table.add_column("Status", style="cyan")
        table.add_column("Selected", style="green")
        
        for project in projects:
            project_id = project.get("id", project.get("project_id", ""))
            project_name = project.get("name", project.get("project_name", ""))
            status = project.get("status", "-")
            is_selected = "✓" if project_id == workspace_project_id else ""
            
            table.add_row(project_id, project_name, status, is_selected)
        
        console.print(table)
        console.print()
        
        if workspace_project_id:
            console.print(f"[green]✓[/green] Workspace linked to: {workspace_project_id}")
        else:
            console.print("[dim]No workspace link. Select with: fluxloop projects select <id>[/dim]")
        
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to list projects: {e}", style="bold red")
        raise typer.Exit(1)


@app.command("create")
def create_project(
    name: str = typer.Option(
        ..., "--name", "-n", help="Project name"
    ),
    description: Optional[str] = typer.Option(
        None, "--description", "-d", help="Project description"
    ),
    workspace_id: Optional[str] = typer.Option(
        None, "--workspace-id", help="Workspace ID (required if multiple)"
    ),
    language: Optional[str] = typer.Option(
        None, "--language", "-l",
        help="Default language code for this project (e.g., ko, en, ja)",
    ),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
    select: bool = typer.Option(
        True, "--select/--no-select", help="Automatically select the created project"
    ),
):
    """
    Create a new Web Project.
    """
    api_url = resolve_api_url(api_url, staging=staging)

    try:
        client = create_authenticated_client(api_url, use_jwt=True)

        resolved_workspace_id = _resolve_workspace_id(client, workspace_id)
        payload = {"name": name, "workspace_id": resolved_workspace_id}
        if description:
            payload["description"] = description

        normalized_lang = normalize_language_token(language)
        if normalized_lang:
            payload["settings"] = {"default_language": normalized_lang}
        
        console.print(f"[cyan]Creating project '{name}'...[/cyan]")
        
        resp = post_with_retry(client, "/api/projects", payload=payload)
        handle_api_error(resp, "create project")
        
        data = resp.json()
        project_id = data.get("id", data.get("project_id", ""))
        project_name = data.get("name", data.get("project_name", name))
        
        console.print(f"[green]✓[/green] Project created: {project_name} ([dim]{project_id}[/dim])")
        print(f"PROJECT_CREATED: {project_id}")
        
        # Automatically select if requested
        if select:
            select_web_project(project_id, project_name, api_url)
            console.print(f"[green]✓[/green] Project selected for this workspace")
        
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to create project: {e}", style="bold red")
        raise typer.Exit(1)


@app.command("select")
def select_project(
    project_id: str = typer.Argument(..., help="Project ID to select"),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    Select a Web Project for this workspace.
    
    This command:
    1. Verifies the project exists on the Web
    2. Creates/updates .fluxloop/project.json (workspace ↔ Web Project link)
    
    Use this to connect your local workspace to a Web Project before
    creating scenarios with 'fluxloop init scenario'.
    """
    api_url = resolve_api_url(api_url, staging=staging)
    
    try:
        # Verify project exists by fetching it
        client = create_authenticated_client(api_url, use_jwt=True)
        resp = client.get(f"/api/projects/{project_id}")
        handle_api_error(resp, "get project")
        
        data = resp.json()
        project_name = data.get("name", data.get("project_name", project_id))
        
        # Create/update project.json (workspace ↔ Web Project link)
        select_web_project(project_id, project_name, api_url)
        
        console.print(f"[green]✓[/green] Selected Web Project: {project_name}")
        console.print(f"  Project ID: [dim]{project_id}[/dim]")
        console.print(f"  Workspace linked: [dim].fluxloop/project.json[/dim]")
        print(f"PROJECT_SELECTED: {project_id}")
        print(f"PROJECT_NAME: {project_name}")
        
        console.print()
        console.print("[dim]Next: Create a scenario with 'fluxloop init scenario --name <name>'[/dim]")
        
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to select project: {e}", style="bold red")
        raise typer.Exit(1)


@app.command("show")
def show_project(
    project_id: Optional[str] = typer.Argument(
        None, help="Project ID (defaults to current context)"
    ),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    Show details of a project.
    """
    api_url = resolve_api_url(api_url, staging=staging)
    
    # Use current context if no project_id provided
    if not project_id:
        project_id = get_current_project_id()
        if not project_id:
            console.print("[yellow]No project selected.[/yellow]")
            console.print("[dim]Select one with: fluxloop projects select <id>[/dim]")
            raise typer.Exit(1)
    
    try:
        client = create_authenticated_client(api_url, use_jwt=True)
        resp = client.get(f"/api/projects/{project_id}")
        handle_api_error(resp, "get project")
        
        data = resp.json()
        
        console.print(f"\n[bold]Project Details[/bold]")
        console.print(f"  ID: [dim]{data.get('id', data.get('project_id', ''))}[/dim]")
        console.print(f"  Name: [bold]{data.get('name', data.get('project_name', ''))}[/bold]")
        
        if data.get("description"):
            console.print(f"  Description: {data.get('description')}")
        if data.get("status"):
            console.print(f"  Status: {data.get('status')}")
        if data.get("created_at"):
            console.print(f"  Created: {data.get('created_at')}")
        
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to get project: {e}", style="bold red")
        raise typer.Exit(1)
