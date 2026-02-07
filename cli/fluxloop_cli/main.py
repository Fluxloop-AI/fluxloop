"""
Main CLI application entry point.
"""

from typing import Optional
import warnings

import typer
from rich.console import Console
from rich.panel import Panel

from . import __version__
from .commands import (
    apikeys,
    auth,
    bundles,
    config,
    context,
    criteria,
    data,
    evaluate,
    generate,
    init,
    inputs,
    local_context,
    personas,
    projects,
    run,
    scenarios,
    status,
    sync,
    test,
)

# Suppress known noisy warnings from dependencies to keep CLI output clean.
warnings.filterwarnings(
    "ignore",
    message=r"The package `typer==.*` does not have an extra named `all`",
    category=UserWarning,
)
warnings.filterwarnings(
    "ignore",
    message=r"Field name \"schema\" in \".*\" shadows an attribute in parent \"BaseModel\"",
    category=UserWarning,
)
warnings.filterwarnings(
    "ignore",
    message=r"Field name \"schema\" in \".*\" shadows an attribute in parent \"ValidatedFunction.*\"",
    category=UserWarning,
)

# Create the main Typer app
app = typer.Typer(
    name="fluxloop",
    help="FluxLoop CLI - Run simulations and manage experiments for AI agents",
    add_completion=True,
    rich_markup_mode="rich",
)

# Create console for rich output
console = Console()

# Add subcommands
app.add_typer(init.app, name="init", help="Initialize a new FluxLoop project")
app.add_typer(run.app, name="run", help="Run simulations and experiments")
app.add_typer(status.app, name="status", help="Check status and view results")
app.add_typer(config.app, name="config", help="Manage configuration")
app.add_typer(generate.app, name="generate", help="Generate input datasets")
app.add_typer(sync.app, name="sync", help="Sync bundles and upload results")
app.add_typer(criteria.app, name="criteria", help="Show pulled evaluation criteria")
app.add_typer(evaluate.app, name="evaluate", help="Trigger server-side evaluation")
app.add_typer(test.app, name="test", help="Run pull -> run -> upload test workflow")
app.add_typer(auth.app, name="auth", help="Manage authentication")
app.add_typer(apikeys.app, name="apikeys", help="Manage API Keys for sync operations")
app.add_typer(projects.app, name="projects", help="Manage projects")
app.add_typer(scenarios.app, name="scenarios", help="Manage test scenarios")
app.add_typer(context.app, name="intent", help="Refine intent and context")  # renamed to avoid conflict
app.add_typer(local_context.app, name="context", help="Manage local working context")
app.add_typer(personas.app, name="personas", help="Manage test personas")
app.add_typer(inputs.app, name="inputs", help="Synthesize and manage test inputs")
app.add_typer(bundles.app, name="bundles", help="Manage test bundles")
app.add_typer(data.app, name="data", help="Manage project data (Knowledge & Datasets)")


def version_callback(value: bool):
    """Show version and exit."""
    if value:
        console.print(f"[bold blue]FluxLoop CLI[/bold blue] version [green]{__version__}[/green]")
        raise typer.Exit()


@app.callback()
def main(
    ctx: typer.Context,
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        "-v",
        help="Show version and exit",
        callback=version_callback,
        is_eager=True,
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        help="Enable verbose output",
    ),
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Enable debug mode",
    ),
):
    """
    FluxLoop CLI - Simulation and observability for AI agents.
    
    Use [bold]fluxloop --help[/bold] to see available commands.
    """
    # Store global options in context
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["debug"] = debug
    
    if debug:
        console.print("[dim]Debug mode enabled[/dim]")


@app.command()
def hello(
    name: str = typer.Argument("World", help="Name to greet"),
):
    """
    Simple hello command to test the CLI.
    """
    console.print(
        Panel(
            f"[bold green]Hello, {name}![/bold green]\n\n"
            f"Welcome to FluxLoop CLI v{__version__}",
            title="[bold blue]FluxLoop[/bold blue]",
            border_style="blue",
        )
    )


if __name__ == "__main__":
    app()
