"""
Config command for managing configuration.
"""

import os
import fluxloop
from pathlib import Path
from typing import Dict, Optional

import typer
import yaml
from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table

from ..config_loader import load_experiment_config
from ..constants import DEFAULT_CONFIG_PATH, DEFAULT_ROOT_DIR_NAME
from ..config_schema import CONFIG_SECTION_FILENAMES
from ..project_paths import (
    resolve_config_path,
    resolve_env_path,
)

app = typer.Typer()
console = Console()


@app.command()
def show(
    config_file: Path = typer.Option(
        DEFAULT_CONFIG_PATH,
        "--file",
        "-f",
        help="Configuration file to show",
    ),
    project: Optional[str] = typer.Option(
        None,
        "--project",
        help="Project name under the FluxLoop root",
    ),
    root: Path = typer.Option(
        Path(DEFAULT_ROOT_DIR_NAME),
        "--root",
        help="FluxLoop root directory",
    ),
    format: str = typer.Option(
        "yaml",
        "--format",
        help="Output format (yaml, json)",
    ),
):
    """
    Show current configuration.
    """
    resolved_path = resolve_config_path(config_file, project, root)
    if not resolved_path.exists():
        console.print(f"[red]Error:[/red] Configuration file not found: {config_file}")
        raise typer.Exit(1)

    content = resolved_path.read_text()

    if format == "json":
        # Convert YAML to JSON
        import json
        data = yaml.safe_load(content)
        content = json.dumps(data, indent=2)
        lexer = "json"
    else:
        lexer = "yaml"

    syntax = Syntax(content, lexer, theme="monokai", line_numbers=True)
    console.print(syntax)


@app.command()
def set(
    key: str = typer.Argument(
        ...,
        help="Configuration key to set (e.g., iterations, runner.timeout)",
    ),
    value: str = typer.Argument(
        ...,
        help="Value to set",
    ),
    config_file: Path = typer.Option(
        DEFAULT_CONFIG_PATH,
        "--file",
        "-f",
        help="Configuration file to update",
    ),
    project: Optional[str] = typer.Option(
        None,
        "--project",
        help="Project name under the FluxLoop root",
    ),
    root: Path = typer.Option(
        Path(DEFAULT_ROOT_DIR_NAME),
        "--root",
        help="FluxLoop root directory",
    ),
):
    """
    Set a configuration value.
    
    Examples:
    - fluxloop config set iterations 20
    - fluxloop config set runner.timeout 300
    """
    resolved_path = resolve_config_path(config_file, project, root)
    if not resolved_path.exists():
        console.print(f"[red]Error:[/red] Configuration file not found: {config_file}")
        raise typer.Exit(1)

    # Load configuration
    with open(resolved_path) as f:
        config = yaml.safe_load(f) or {}

    # Parse key path
    keys = key.split(".")
    current = config
    
    # Navigate to the key
    for k in keys[:-1]:
        if k not in current:
            current[k] = {}
        current = current[k]
    
    # Set the value
    final_key = keys[-1]
    
    # Try to parse value as appropriate type
    try:
        # Try as number
        if "." in value:
            parsed_value = float(value)
        else:
            parsed_value = int(value)
    except ValueError:
        # Try as boolean
        if value.lower() in ("true", "false"):
            parsed_value = value.lower() == "true"
        else:
            # Keep as string
            parsed_value = value
    
    current[final_key] = parsed_value

    # Save configuration
    with open(resolved_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    
    console.print(f"[green]✓[/green] Set {key} = {parsed_value}")


@app.command()
def env(
    show_values: bool = typer.Option(
        False,
        "--show-values",
        "-s",
        help="Show actual values (be careful with secrets)",
    ),
    project: Optional[str] = typer.Option(
        None,
        "--project",
        help="Project name under the FluxLoop root",
    ),
    root: Path = typer.Option(
        Path(DEFAULT_ROOT_DIR_NAME),
        "--root",
        help="FluxLoop root directory",
    ),
):
    """
    Show environment variables used by FluxLoop.
    """
    # Load environment files so values reflect merged root -> project overrides
    try:
        from ..project_paths import resolve_root_dir, resolve_project_dir
        loaded_paths = []
        root_env = resolve_root_dir(root) / ".env"
        if root_env.exists():
            fluxloop.load_env(root_env, override=True, refresh_config=False)
            loaded_paths.append(str(root_env))
        if project:
            project_env = resolve_project_dir(project, root) / ".env"
            if project_env.exists():
                fluxloop.load_env(project_env, override=True, refresh_config=False)
                loaded_paths.append(str(project_env))
    except Exception:
        loaded_paths = []

    env_vars = [
        ("FLUXLOOP_API_URL", "API base URL override (optional, project.json is primary)", None),
        ("FLUXLOOP_SYNC_API_KEY", "Sync API key for uploads", None),
        ("FLUXLOOP_ENABLED", "Enable/disable tracing", "true"),
        ("FLUXLOOP_DEBUG", "Enable debug mode", "false"),
        ("FLUXLOOP_SAMPLE_RATE", "Trace sampling rate (0-1)", "1.0"),
        ("FLUXLOOP_SERVICE_NAME", "Service name for traces", None),
        ("FLUXLOOP_ENVIRONMENT", "Environment (dev/staging/prod)", "development"),
        ("OPENAI_API_KEY", "OpenAI API key", None),
        ("ANTHROPIC_API_KEY", "Anthropic API key", None),
    ]
    
    table = Table(title="FluxLoop Environment Variables")
    table.add_column("Variable", style="cyan")
    table.add_column("Description")
    table.add_column("Current Value")
    table.add_column("Default", style="dim")
    
    for var_name, description, default in env_vars:
        current = os.getenv(var_name)
        
        if current:
            if show_values or not var_name.endswith("_KEY"):
                display_value = current
            else:
                # Mask API keys
                display_value = "****" + current[-4:] if len(current) > 4 else "****"
            display_value = f"[green]{display_value}[/green]"
        else:
            display_value = "[yellow]Not set[/yellow]"
        
        table.add_row(
            var_name,
            description,
            display_value,
            default or "-"
        )
    
    console.print(table)
    
    # Show loaded env sources (root first, then project)
    if loaded_paths:
        console.print("\n[dim]Loaded from:[/dim] " + ", ".join(loaded_paths))
    else:
        console.print("\n[yellow]No .env file found[/yellow]")
        console.print("Create one with: [cyan]fluxloop init project[/cyan]")


@app.command()
def set_llm(
    provider: str = typer.Argument(..., help="LLM provider identifier (e.g., openai, anthropic, gemini)"),
    api_key: str = typer.Argument(..., help="API key or token for the provider"),
    model: Optional[str] = typer.Option(None, "--model", "-m", help="Default model to use"),
    overwrite_env: bool = typer.Option(False, "--overwrite-env", help="Overwrite existing key in .env"),
    config_file: Path = typer.Option(
        Path(CONFIG_SECTION_FILENAMES["input"]),
        "--file",
        "-f",
        help="Configuration file to update",
    ),
    env_file: Path = typer.Option(Path(".env"), "--env-file", help="Path to environment file"),
    project: Optional[str] = typer.Option(None, "--project", help="Project name under the FluxLoop root"),
    root: Path = typer.Option(Path(DEFAULT_ROOT_DIR_NAME), "--root", help="FluxLoop root directory"),
):
    """Configure LLM provider credentials and defaults."""

    supported_providers: Dict[str, Dict[str, str]] = {
        "openai": {
            "env_var": "OPENAI_API_KEY",
            "model": "gpt-5",
        },
        "anthropic": {
            "env_var": "ANTHROPIC_API_KEY",
            "model": "claude-3-haiku-20240307",
        },
        "gemini": {
            "env_var": "GEMINI_API_KEY",
            "model": "gemini-2.5-flash",
        },
    }

    normalized_provider = provider.lower()
    if normalized_provider not in supported_providers:
        available = ", ".join(sorted(supported_providers.keys()))
        console.print(
            f"[red]Error:[/red] Unsupported provider '{provider}'. Available: {available}"
        )
        raise typer.Exit(1)

    provider_info = supported_providers[normalized_provider]
    env_var = provider_info["env_var"]

    # Update .env file
    env_path = resolve_env_path(env_file, project, root)
    env_contents: Dict[str, str] = {}
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env_contents[key.strip()] = value.strip()

    if env_var in env_contents and not overwrite_env:
        console.print(
            f"[yellow]Warning:[/yellow] {env_var} already set. Use --overwrite-env to replace it."
        )
    else:
        env_contents[env_var] = api_key
        env_lines = [f"{key}={value}" for key, value in env_contents.items()]
        env_path.write_text("\n".join(env_lines) + "\n")
        console.print(f"[green]✓[/green] Saved {env_var} to {env_path}")

    # Update configuration file
    resolved_cfg = resolve_config_path(config_file, project, root)
    with open(resolved_cfg) as f:
        config = yaml.safe_load(f) or {}

    input_generation = config.setdefault("input_generation", {})

    llm_config = input_generation.setdefault("llm", {})
    llm_config["enabled"] = True
    llm_config["provider"] = normalized_provider
    llm_config["model"] = model or llm_config.get("model", provider_info["model"])

    with open(resolved_cfg, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    display_config_path = resolved_cfg if project else config_file
    console.print(
        f"[green]✓[/green] Updated {display_config_path} with provider='{normalized_provider}' model='{llm_config['model']}'"
    )


@app.command()
def set_sync_key(
    api_key: str = typer.Argument(..., help="Sync API key"),
    overwrite_env: bool = typer.Option(False, "--overwrite-env", help="Overwrite existing key in .env"),
    env_file: Path = typer.Option(Path(".env"), "--env-file", help="Path to environment file"),
    project: Optional[str] = typer.Option(None, "--project", help="Project name under the FluxLoop root"),
    root: Path = typer.Option(Path(DEFAULT_ROOT_DIR_NAME), "--root", help="FluxLoop root directory"),
):
    """Save Sync API key to .env."""

    env_path = resolve_env_path(env_file, project, root)
    env_contents: Dict[str, str] = {}
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env_contents[key.strip()] = value.strip()

    if "FLUXLOOP_SYNC_API_KEY" in env_contents and not overwrite_env:
        console.print(
            "[yellow]Warning:[/yellow] FLUXLOOP_SYNC_API_KEY already set. Use --overwrite-env to replace it."
        )
    else:
        env_contents["FLUXLOOP_SYNC_API_KEY"] = api_key

    env_lines = [f"{key}={value}" for key, value in env_contents.items()]
    env_path.write_text("\n".join(env_lines) + "\n")
    console.print(f"[green]✓[/green] Saved sync API key to {env_path}")


@app.command()
def validate(
    config_file: Path = typer.Option(
        DEFAULT_CONFIG_PATH,
        "--file",
        "-f",
        help="Configuration file to validate",
    ),
    project: Optional[str] = typer.Option(
        None,
        "--project",
        help="Project name under the FluxLoop root",
    ),
    root: Path = typer.Option(
        Path(DEFAULT_ROOT_DIR_NAME),
        "--root",
        help="FluxLoop root directory",
    ),
):
    """
    Validate configuration file.
    """
    resolved_path = resolve_config_path(config_file, project, root)
    if not resolved_path.exists():
        console.print(f"[red]Error:[/red] Configuration file not found: {config_file}")
        raise typer.Exit(1)
    
    try:
        config = load_experiment_config(
            resolved_path,
            project=project,
            root=root,
        )
        
        # Show validation results
        console.print("[green]✓[/green] Configuration is valid!\n")
        
        # Show summary
        table = Table(show_header=False)
        table.add_column("Property", style="cyan")
        table.add_column("Value")
        
        table.add_row("Experiment Name", config.name)
        table.add_row("Iterations", str(config.iterations))
        table.add_row("Personas", str(len(config.personas)))
        table.add_row("Variations", str(config.variation_count))
        table.add_row("Total Runs", str(config.estimate_total_runs()))
        table.add_row("Runner Module", config.runner.module_path)
        table.add_row("Evaluators", str(len(config.evaluators)))
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[red]✗ Validation failed:[/red] {e}")
        raise typer.Exit(1)
