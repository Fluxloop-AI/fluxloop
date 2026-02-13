"""
API Key management commands for FluxLoop CLI.

Creates and manages API Keys for sync operations (pull/upload).
User-scoped JWT is used to create project-scoped API Keys.
"""

import os
from pathlib import Path
from typing import Dict, Optional

import typer
from rich.console import Console

from ..constants import DEFAULT_ROOT_DIR_NAME, FLUXLOOP_DIR_NAME
from ..http_client import create_authenticated_client, post_with_retry
from ..api_utils import handle_api_error, resolve_api_url
from ..context_manager import get_current_project_id
from ..project_paths import resolve_env_path, find_workspace_root

app = typer.Typer(help="Manage API Keys for sync operations")
console = Console()


def _get_workspace_env_path() -> Optional[Path]:
    """Get the workspace-level .env path (.fluxloop/.env).
    
    API keys are project-scoped, so they're stored at workspace level
    and shared by all scenarios.
    """
    workspace_root = find_workspace_root()
    if workspace_root:
        return workspace_root / FLUXLOOP_DIR_NAME / ".env"
    # Fall back to current directory
    cwd_fluxloop = Path.cwd() / FLUXLOOP_DIR_NAME
    if cwd_fluxloop.exists():
        return cwd_fluxloop / ".env"
    return None


def _check_existing_api_key(env_file: Path) -> Optional[str]:
    """Check if API Key already exists in .env file."""
    if not env_file.exists():
        return None
    
    for line in env_file.read_text().splitlines():
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() == "FLUXLOOP_SYNC_API_KEY":
            return value.strip() if value.strip() else None
    
    return None


def _save_api_key_to_env(api_key: str, env_file: Path, overwrite: bool = False) -> bool:
    """Save API Key to .env file. Returns True if saved."""
    env_contents: Dict[str, str] = {}
    
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env_contents[key.strip()] = value.strip()
    
    if "FLUXLOOP_SYNC_API_KEY" in env_contents and not overwrite:
        return False
    
    env_contents["FLUXLOOP_SYNC_API_KEY"] = api_key
    env_file.parent.mkdir(parents=True, exist_ok=True)
    env_lines = [f"{k}={v}" for k, v in env_contents.items()]
    env_file.write_text("\n".join(env_lines) + "\n")
    return True


def _extract_api_key_from_response(data: dict) -> Optional[str]:
    """Extract API key from response, handling different formats."""
    # Try direct "key" field
    if isinstance(data.get("key"), str):
        return data["key"]
    
    # Try "api_key" field - could be string or dict
    api_key_field = data.get("api_key")
    if isinstance(api_key_field, str):
        return api_key_field
    if isinstance(api_key_field, dict):
        inner_key = api_key_field.get("key")
        if isinstance(inner_key, str):
            return inner_key
    
    # Try "api_key_data" field
    api_key_data = data.get("api_key_data")
    if isinstance(api_key_data, dict):
        inner_key = api_key_data.get("key")
        if isinstance(inner_key, str):
            return inner_key
    
    return None


@app.command("create")
def create_api_key(
    project_id: Optional[str] = typer.Option(None, "--project-id", help="Project ID"),
    name: Optional[str] = typer.Option("cli-generated", "--name", "-n", help="Key name"),
    save: bool = typer.Option(True, "--save/--no-save", help="Save to .env"),
    env_file: Optional[Path] = typer.Option(None, "--env-file", help="Override env file path"),
    overwrite_env: bool = typer.Option(False, "--overwrite-env", help="Overwrite existing"),
    project: Optional[str] = typer.Option(None, "--project", help="Project name (legacy)"),
    root: Path = typer.Option(Path(DEFAULT_ROOT_DIR_NAME), "--root", help="Root dir"),
    api_url: Optional[str] = typer.Option(None, "--api-url", help="API base URL"),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """Create an API Key for sync operations (pull/upload).
    
    Saves to .fluxloop/.env (workspace level) by default.
    API keys are project-scoped and shared by all scenarios.
    """
    api_url = resolve_api_url(api_url, staging=staging)
    
    # Priority: --env-file > workspace .fluxloop/.env > legacy resolution
    if env_file:
        env_path = env_file.expanduser().resolve()
    else:
        env_path = _get_workspace_env_path()
        if env_path:
            console.print(f"[dim]Saving to workspace: {env_path}[/dim]")
        else:
            env_path = resolve_env_path(Path(".env"), project, root)
            console.print(f"[dim]No workspace found, saving to: {env_path}[/dim]")
    
    if not project_id:
        project_id = get_current_project_id()
        if not project_id:
            console.print("[yellow]No project selected.[/yellow]")
            console.print("[dim]Select: fluxloop projects select <id>[/dim]")
            raise typer.Exit(1)
    
    # Pre-check existing key
    if save and not overwrite_env:
        existing = _check_existing_api_key(env_path)
        if existing:
            console.print(f"[yellow]⚠[/yellow] Key already exists in {env_path}")
            console.print("[dim]Use --overwrite-env to replace[/dim]")
            raise typer.Exit(1)
    
    try:
        client = create_authenticated_client(api_url, use_jwt=True)
        payload = {"project_id": project_id, "name": name}
        
        console.print(f"[cyan]Creating API Key for project: {project_id}...[/cyan]")
        resp = post_with_retry(client, "/api/api-keys", payload=payload)
        handle_api_error(resp, "create API key")
        
        data = resp.json()
        api_key = _extract_api_key_from_response(data)
        
        if not api_key:
            console.print("[red]✗[/red] Key not found in response", style="bold red")
            console.print(f"[dim]Response: {data}[/dim]")
            raise typer.Exit(1)
        
        masked = "****" + api_key[-8:] if len(api_key) > 8 else "****"
        console.print(f"[green]✓[/green] API Key created: [bold]{masked}[/bold]")
        
        if save:
            saved = _save_api_key_to_env(api_key, env_path, overwrite_env)
            if saved:
                console.print(f"[green]✓[/green] Saved to {env_path}")
            else:
                console.print(f"[yellow]⚠[/yellow] Could not save")
                console.print(f"\n[bold]API Key:[/bold] {api_key}")
                console.print("[dim]Save manually![/dim]")
        else:
            console.print(f"\n[bold]API Key:[/bold] {api_key}")
            console.print("[dim]Save securely![/dim]")
        
        console.print("\n[dim]Now use: fluxloop sync pull[/dim]")
        
    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"[red]✗[/red] Failed: {e}", style="bold red")
        raise typer.Exit(1)


@app.command("check")
def check_api_key(
    env_file: Optional[Path] = typer.Option(None, "--env-file", help="Override env file"),
    project: Optional[str] = typer.Option(None, "--project", help="Project name (legacy)"),
    root: Path = typer.Option(Path(DEFAULT_ROOT_DIR_NAME), "--root", help="Root dir"),
):
    """Check if API Key is configured.
    
    Checks in order: environment variable → .fluxloop/.env → legacy paths
    """
    # 1. Check environment variable
    env_key = os.getenv("FLUXLOOP_SYNC_API_KEY") or os.getenv("FLUXLOOP_API_KEY")
    if env_key and env_key != "your-api-key-here":
        masked = "****" + env_key[-4:] if len(env_key) > 4 else "****"
        console.print(f"[green]✓[/green] API Key set: {masked}")
        console.print("[dim]Source: environment variable[/dim]")
        return
    
    # 2. Check workspace .fluxloop/.env
    workspace_env = _get_workspace_env_path()
    if workspace_env and workspace_env.exists():
        for line in workspace_env.read_text().splitlines():
            if line.startswith("FLUXLOOP_SYNC_API_KEY=") or line.startswith("FLUXLOOP_API_KEY="):
                val = line.split("=", 1)[1].strip()
                if val and val != "your-api-key-here":
                    masked = "****" + val[-4:] if len(val) > 4 else "****"
                    console.print(f"[green]✓[/green] API Key set: {masked}")
                    console.print(f"[dim]Source: {workspace_env}[/dim]")
                    return
    
    # 3. Check explicit env file or legacy path
    if env_file:
        env_path = env_file.expanduser().resolve()
    else:
        env_path = resolve_env_path(Path(".env"), project, root)
    
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("FLUXLOOP_SYNC_API_KEY=") or line.startswith("FLUXLOOP_API_KEY="):
                val = line.split("=", 1)[1].strip()
                if val and val != "your-api-key-here":
                    masked = "****" + val[-4:] if len(val) > 4 else "****"
                    console.print(f"[green]✓[/green] API Key set: {masked}")
                    console.print(f"[dim]Source: {env_path}[/dim]")
                    return
    
    console.print("[yellow]✗[/yellow] API Key not set")
    console.print("[dim]Create: fluxloop apikeys create[/dim]")
