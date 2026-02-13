"""
Input synthesis and management commands for FluxLoop CLI.

Note: This is separate from 'generate inputs' which does local generation.
These commands interact with the Web API for synthesis.
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
from ..context_manager import get_current_scenario_id, get_current_web_project_id

app = typer.Typer(help="Synthesize and manage test inputs via Web API")
console = Console()

DEFAULT_SYNTHESIZE_TIMEOUT = 180.0


def _get_default_timeout() -> float:
    """Get default timeout from environment variable or use built-in default."""
    env_timeout = os.getenv("FLUXLOOP_SYNTHESIZE_TIMEOUT")
    if env_timeout:
        try:
            return float(env_timeout)
        except ValueError:
            pass
    return DEFAULT_SYNTHESIZE_TIMEOUT


@app.command()
def synthesize(
    project_id: Optional[str] = typer.Option(
        None, "--project-id", help="Project ID (defaults to current context)"
    ),
    scenario_id: Optional[str] = typer.Option(
        None, "--scenario-id", help="Scenario ID for synthesis (defaults to current context)"
    ),
    persona_ids: Optional[str] = typer.Option(
        None, "--persona-ids", help="Comma-separated persona IDs"
    ),
    total_count: Optional[int] = typer.Option(
        None, "--total-count", help="Total number of inputs to generate"
    ),
    count_per_persona: Optional[int] = typer.Option(
        None, "--count-per-persona", help="Number of inputs per persona"
    ),
    include_data_context: Optional[bool] = typer.Option(
        None,
        "--include-data-context/--no-include-data-context",
        help="Include scenario data summary as grounding context (default: true)",
    ),
    grounding_test_ratio: Optional[float] = typer.Option(
        None,
        "--grounding-ratio",
        "--grounding-test-ratio",
        help="Ratio of grounding tests when data context is available (0-1, default 0.3)",
    ),
    bundle_version_id: Optional[str] = typer.Option(
        None, "--bundle-version-id", help="Bundle version ID to use for synthesis"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview without saving to database"
    ),
    file: Optional[Path] = typer.Option(
        None, "--file", "-f", help="Load payload from YAML or JSON file"
    ),
    timeout_seconds: Optional[float] = typer.Option(
        None,
        "--timeout",
        help="Request timeout in seconds (default: 180, or FLUXLOOP_SYNTHESIZE_TIMEOUT env)",
    ),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    Synthesize test inputs using Web API.
    
    Uses current project/scenario from context if not specified.
    """
    effective_timeout = timeout_seconds or _get_default_timeout()
    api_url = resolve_api_url(api_url, staging=staging)
    
    # Use context if no project_id specified
    if not project_id:
        project_id = get_current_web_project_id()
        if not project_id:
            console.print("[yellow]No Web Project selected.[/yellow]")
            console.print("[dim]Select one with: fluxloop projects select <id>[/dim]")
            raise typer.Exit(1)
    
    # Use context if no scenario_id specified
    if not scenario_id:
        scenario_id = get_current_scenario_id()
        if not scenario_id:
            console.print("[yellow]No scenario selected.[/yellow]")
            console.print("[dim]Select one with: fluxloop scenarios select <id>[/dim]")
            raise typer.Exit(1)

    # Build payload
    payload: Dict[str, Any] = {
        "project_id": project_id,
        "scenario_id": scenario_id,
        "dry_run": dry_run,
    }

    if persona_ids:
        persona_id_list: List[str] = [pid.strip() for pid in persona_ids.split(",") if pid.strip()]
        payload["persona_ids"] = persona_id_list
    else:
        suggested_path = Path.home() / ".fluxloop" / "personas" / f"suggested_{scenario_id}.yaml"
        if suggested_path.exists():
            try:
                suggested_payload = load_payload_file(suggested_path)
                suggested_ids = suggested_payload.get("persona_ids")
                if isinstance(suggested_ids, list):
                    suggested_ids = [
                        pid for pid in suggested_ids if isinstance(pid, str) and pid
                    ]
                else:
                    suggested_ids = []
                if not suggested_ids:
                    personas_payload = suggested_payload.get("personas")
                    if isinstance(personas_payload, list):
                        suggested_ids = [
                            persona.get("id")
                            for persona in personas_payload
                            if isinstance(persona, dict)
                            and isinstance(persona.get("id"), str)
                        ]
                if suggested_ids:
                    payload["persona_ids"] = suggested_ids
                    console.print(
                        f"[dim]Using suggested personas ({len(suggested_ids)}) from {suggested_path}[/dim]"
                    )
            except Exception as e:
                console.print(f"[yellow]Failed to load suggested personas: {e}[/yellow]")

    if total_count is not None:
        payload["total_count"] = total_count

    if count_per_persona is not None:
        payload["count_per_persona"] = count_per_persona

    if include_data_context is not None:
        payload["include_data_context"] = include_data_context

    if grounding_test_ratio is not None:
        payload["grounding_test_ratio"] = grounding_test_ratio

    if bundle_version_id:
        payload["bundle_version_id"] = bundle_version_id

    # Override with file if provided
    if file:
        file_data = load_payload_file(file)
        payload.update(file_data)

    try:
        console.print("[cyan]Synthesizing test inputs...[/cyan]")
        if effective_timeout > 60:
            console.print(f"[dim]Timeout: {effective_timeout:.0f}s[/dim]")

        client = create_authenticated_client(api_url, use_jwt=True, timeout=effective_timeout)
        resp = post_with_retry(client, "/api/inputs/synthesize", payload=payload)

        handle_api_error(resp, f"input synthesis for scenario {scenario_id}")

        data = resp.json()

        # Display results
        console.print()
        if dry_run:
            console.print("[yellow]Dry-run mode: No data saved[/yellow]")

        # Show persona breakdown
        if "persona_breakdown" in data:
            breakdown = data["persona_breakdown"]
            for persona_info in breakdown:
                persona_name = persona_info.get("persona_name", "Unknown")
                count = persona_info.get("count", 0)
                console.print(f"[green]✓[/green] Persona: {persona_name} ({count} inputs)")

        # Show total count (fallback to item list when API omits total_count)
        total_generated = data.get("total_count")
        if total_generated is None:
            for key in ("items", "input_items", "inputs"):
                items = data.get(key)
                if isinstance(items, list):
                    total_generated = len(items)
                    break
        if total_generated is None:
            total_generated = 0
        console.print()
        console.print(f"[bold]Synthesis complete: {total_generated} inputs generated[/bold]")

        # Show input_set_id (support both 'id' and 'input_set_id' field names)
        result_input_set_id = data.get("input_set_id") or data.get("id")
        if result_input_set_id:
            console.print(f"  Input Set ID: [bold cyan]{result_input_set_id}[/bold cyan]")

        # Show preview
        if "samples" in data and data["samples"]:
            console.print("\n[bold]Preview:[/bold]")
            for i, sample in enumerate(data["samples"][:5], 1):  # Show max 5 samples
                persona_id = sample.get("persona_id", "N/A")
                text = sample.get("text", sample.get("input", "N/A"))

                # Truncate long text
                if len(text) > 150:
                    text = text[:150] + "..."

                console.print(f"  {i}. [dim][{persona_id}][/dim] \"{text}\"")

        # Save to local cache
        if not dry_run:
            cache_path = save_cache_file(
                "inputs", "generated.yaml", data
            )
            console.print(f"\n[dim]Saved to: {cache_path}[/dim]")

        # Next steps
        if result_input_set_id and not dry_run:
            console.print("\n[bold]Next steps:[/bold]")
            console.print(
                f"  [dim]1. Quality check: fluxloop inputs qc --scenario-id {scenario_id} --input-set-id {result_input_set_id}[/dim]"
            )
            console.print(
                f"  [dim]2. Create bundle: fluxloop bundles publish --scenario-id {scenario_id} --input-set-id {result_input_set_id}[/dim]"
            )

    except httpx.TimeoutException:
        console.print(
            f"[red]✗[/red] Synthesis timed out after {effective_timeout:.0f}s.\n"
            "  Options:\n"
            "    --timeout 300              (increase timeout)\n"
            "    --total-count N            (reduce inputs)\n"
            "    FLUXLOOP_SYNTHESIZE_TIMEOUT=300  (env default)",
            style="bold red",
        )
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]✗[/red] Synthesis failed: {e}", style="bold red")
        raise typer.Exit(1)


@app.command()
def refine(
    scenario_id: str = typer.Option(..., "--scenario-id", help="Scenario ID"),
    input_set_id: str = typer.Option(..., "--input-set-id", help="Input set ID to refine"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview without saving changes"
    ),
    file: Optional[Path] = typer.Option(
        None, "--file", "-f", help="Load payload from YAML or JSON file"
    ),
    timeout_seconds: Optional[float] = typer.Option(
        None,
        "--timeout",
        help="Request timeout in seconds (default: 180, or FLUXLOOP_SYNTHESIZE_TIMEOUT env)",
    ),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    Refine existing test inputs.
    """
    api_url = resolve_api_url(api_url, staging=staging)
    effective_timeout = timeout_seconds or _get_default_timeout()

    # Build payload
    payload: Dict[str, Any] = {
        "scenario_id": scenario_id,
        "input_set_id": input_set_id,
        "dry_run": dry_run,
    }

    # Override with file if provided
    if file:
        file_data = load_payload_file(file)
        payload.update(file_data)

    try:
        console.print("[cyan]Refining test inputs...[/cyan]")
        if effective_timeout > 60:
            console.print(f"[dim]Timeout: {effective_timeout:.0f}s[/dim]")

        client = create_authenticated_client(api_url, use_jwt=True, timeout=effective_timeout)
        resp = post_with_retry(client, "/api/inputs/refine", payload=payload)

        handle_api_error(resp, f"input refinement for set {input_set_id}")

        data = resp.json()

        console.print()
        console.print("[green]✓[/green] Input refinement complete")

        if dry_run:
            console.print("[yellow]Dry-run mode: No data saved[/yellow]")

        # Show changes
        if "changes" in data:
            changes_count = data.get("changes_count", len(data["changes"]))
            console.print(f"\n[bold]{changes_count} inputs modified[/bold]")

            # Show sample changes
            if data["changes"]:
                console.print("\n[bold]Sample changes:[/bold]")
                for i, change in enumerate(data["changes"][:3], 1):  # Show max 3
                    before = change.get("before", "N/A")
                    after = change.get("after", "N/A")

                    if len(before) > 100:
                        before = before[:100] + "..."
                    if len(after) > 100:
                        after = after[:100] + "..."

                    console.print(f"\n  {i}. [dim]Before:[/dim] {before}")
                    console.print(f"     [dim]After:[/dim]  {after}")

    except httpx.TimeoutException:
        console.print(
            f"[red]✗[/red] Refinement timed out after {effective_timeout:.0f}s.\n"
            "  Options:\n"
            "    --timeout 300              (increase timeout)\n"
            "    FLUXLOOP_SYNTHESIZE_TIMEOUT=300  (env default)",
            style="bold red",
        )
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]✗[/red] Refinement failed: {e}", style="bold red")
        raise typer.Exit(1)


@app.command()
def qc(
    project_id: Optional[str] = typer.Option(
        None, "--project-id", help="Project ID (defaults to current context)"
    ),
    scenario_id: str = typer.Option(..., "--scenario-id", help="Scenario ID"),
    input_set_id: str = typer.Option(..., "--input-set-id", help="Input set ID to check"),
    file: Optional[Path] = typer.Option(
        None, "--file", "-f", help="Load payload from YAML or JSON file"
    ),
    timeout_seconds: Optional[float] = typer.Option(
        None,
        "--timeout",
        help="Request timeout in seconds (default: 180, or FLUXLOOP_SYNTHESIZE_TIMEOUT env)",
    ),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    Run quality checks on test inputs.
    """
    api_url = resolve_api_url(api_url, staging=staging)
    effective_timeout = timeout_seconds or _get_default_timeout()

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
        "input_set_id": input_set_id,
    }

    # Override with file if provided
    if file:
        file_data = load_payload_file(file)
        payload.update(file_data)
        if not payload.get("project_id"):
            payload["project_id"] = project_id

    try:
        console.print("[cyan]Running quality checks...[/cyan]")
        if effective_timeout > 60:
            console.print(f"[dim]Timeout: {effective_timeout:.0f}s[/dim]")

        client = create_authenticated_client(api_url, use_jwt=True, timeout=effective_timeout)
        resp = post_with_retry(client, "/api/inputs/qc", payload=payload)

        handle_api_error(resp, f"quality check for input set {input_set_id}")

        data = resp.json()

        console.print()

        # Format checks
        format_passed = data.get("format_passed", 0)
        format_total = data.get("format_total", 0)
        console.print(f"[green]✓[/green] Format check: {format_passed}/{format_total} passed")

        # Duplicate checks
        duplicates = data.get("duplicates_count", 0)
        if duplicates > 0:
            console.print(f"[yellow]⚠[/yellow] Duplicate check: {duplicates} duplicates found")
        else:
            console.print(f"[green]✓[/green] Duplicate check: No duplicates")

        # Diversity checks
        diversity_issues = data.get("diversity_issues", [])
        if diversity_issues:
            console.print(f"[yellow]⚠[/yellow] Diversity check: {len(diversity_issues)} high similarity pairs")
        else:
            console.print(f"[green]✓[/green] Diversity check: Good diversity")

        # Overall result
        console.print()
        quality_score = data.get("quality_score", "N/A")
        passed_count = data.get("passed_count", format_passed)
        total_count = data.get("total_count", format_total)

        console.print(f"[bold]Quality check result: {passed_count}/{total_count} inputs OK[/bold]")
        if quality_score != "N/A":
            console.print(f"  Quality score: {quality_score}")

        # Show issues
        if diversity_issues:
            console.print("\n[bold]Improvement recommended:[/bold]")
            for issue in diversity_issues[:5]:  # Show max 5
                id1 = issue.get("input_id_1", "N/A")
                id2 = issue.get("input_id_2", "N/A")
                similarity = issue.get("similarity", 0)
                console.print(f"  - {id1} and {id2} similar ({similarity:.0%})")

    except typer.Exit:
        # Keep rich API validation output from handle_api_error intact.
        raise
    except httpx.TimeoutException:
        console.print(
            f"[red]✗[/red] Quality check timed out after {effective_timeout:.0f}s.\n"
            "  Options:\n"
            "    --timeout 300              (increase timeout)\n"
            "    FLUXLOOP_SYNTHESIZE_TIMEOUT=300  (env default)",
            style="bold red",
        )
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]✗[/red] Quality check failed: {e}", style="bold red")
        raise typer.Exit(1)


@app.command("list")
def list_inputs(
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
    List all input sets.
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

        resp = client.get("/api/inputs", params=params)
        handle_api_error(resp, "input sets list")

        data = resp.json()
        input_sets = data if isinstance(data, list) else data.get("input_sets", [])

        if not input_sets:
            console.print("[yellow]No input sets found.[/yellow]")
            if scenario_id:
                console.print(
                    f"[dim]Create inputs: fluxloop inputs synthesize --scenario-id {scenario_id}[/dim]"
                )
            else:
                console.print(
                    "[dim]Create inputs: fluxloop inputs synthesize --scenario-id <id>[/dim]"
                )
            return

        # Create table
        table = Table(title="Input Sets")
        table.add_column("Input Set ID", style="cyan")
        table.add_column("Version", style="bold")
        table.add_column("Created", style="dim")

        for input_set in input_sets:
            # Support both 'id' and 'input_set_id' field names
            set_id = input_set.get("input_set_id") or input_set.get("id", "N/A")
            count = input_set.get("count") or input_set.get("version", 0)
            table.add_row(
                set_id,
                str(count),
                input_set.get("created_at", "N/A"),
            )

        console.print(table)

    except Exception as e:
        console.print(f"[red]✗[/red] List failed: {e}", style="bold red")
        raise typer.Exit(1)


@app.command()
def show(
    input_set_id: str = typer.Option(..., "--input-set-id", help="Input set ID to show"),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    Show input set details with samples.
    """
    api_url = resolve_api_url(api_url, staging=staging)

    try:
        client = create_authenticated_client(api_url, use_jwt=True)
        resp = client.get(f"/api/inputs/{input_set_id}")

        handle_api_error(resp, f"input set {input_set_id}")

        data = resp.json()

        # Show metadata
        console.print(f"\n[bold cyan]Input Set: {input_set_id}[/bold cyan]")
        console.print(f"  Total count: {data.get('count', 'N/A')}")
        console.print(f"  Created: {data.get('created_at', 'N/A')}")

        # Show samples
        if "inputs" in data and data["inputs"]:
            console.print(f"\n[bold]Samples:[/bold]")

            for i, input_item in enumerate(data["inputs"][:10], 1):  # Show max 10
                persona_id = input_item.get("persona_id", "N/A")
                text = input_item.get("text", input_item.get("input", "N/A"))

                # Truncate long text
                if len(text) > 150:
                    text = text[:150] + "..."

                console.print(f"  {i}. [dim][{persona_id}][/dim] {text}")

    except Exception as e:
        console.print(f"[red]✗[/red] Show failed: {e}", style="bold red")
        raise typer.Exit(1)
