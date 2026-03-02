"""
Persona management commands for FluxLoop CLI.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import typer
import yaml
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


def _coerce_stories_payload(raw: Any, *, source: str) -> List[Dict[str, Any]]:
    candidate = raw.get("stories") if isinstance(raw, dict) else raw
    if not isinstance(candidate, list):
        raise typer.BadParameter(
            f"{source} must contain a JSON/YAML list of story objects "
            "or an object with a 'stories' list."
        )
    stories = [item for item in candidate if isinstance(item, dict)]
    if not stories:
        raise typer.BadParameter(f"{source} does not contain any valid story object.")
    return stories


def _compact_text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _normalize_inline_story(raw_story: Dict[str, Any], *, index: int) -> Dict[str, Any]:
    story: Dict[str, Any] = dict(raw_story)

    story_id = _compact_text(story.get("id")) or f"story_{index + 1}"
    title = _compact_text(story.get("title"))
    narrative = _compact_text(story.get("narrative"))
    test_focus = _compact_text(story.get("testFocus"))
    casting_query = _compact_text(story.get("castingQuery"))
    if not title:
        title = narrative or test_focus or casting_query or f"Story {index + 1}"
    if not narrative:
        narrative = f"{title} context and expected user behavior."
    if not test_focus:
        test_focus = "Validate behavior and recovery flow in a realistic scenario."

    profile_raw = story.get("protagonistProfile")
    profile = profile_raw if isinstance(profile_raw, dict) else {}
    key_traits_raw = profile.get("keyTraits")
    key_traits = (
        [item.strip() for item in key_traits_raw if isinstance(item, str) and item.strip()]
        if isinstance(key_traits_raw, list)
        else []
    )
    protagonist_profile = {
        "description": _compact_text(profile.get("description"))
        or "Representative end user in this scenario.",
        "keyTraits": key_traits,
        "idealType": _compact_text(profile.get("idealType")) or "general user",
    }
    if not casting_query:
        casting_query = " ".join(part for part in [title, narrative, test_focus] if part).strip()

    story["id"] = story_id
    story["title"] = title
    story["narrative"] = narrative
    story["testFocus"] = test_focus
    story["protagonistProfile"] = protagonist_profile
    story["castingQuery"] = casting_query
    return story


def _load_stories_from_file(file_path: Path) -> List[Dict[str, Any]]:
    if not file_path.exists():
        raise typer.BadParameter(f"Stories file not found: {file_path}")

    suffix = file_path.suffix.lower()
    content = file_path.read_text()
    try:
        if suffix in {".yaml", ".yml"}:
            parsed = yaml.safe_load(content)
        elif suffix == ".json":
            parsed = json.loads(content)
        else:
            raise typer.BadParameter(
                f"Unsupported stories file format: {suffix}. Use .yaml, .yml, or .json"
            )
    except yaml.YAMLError as exc:
        raise typer.BadParameter(f"Invalid YAML in stories file: {exc}")
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"Invalid JSON in stories file: {exc}")

    return _coerce_stories_payload(parsed, source=str(file_path))


def _parse_inline_stories(raw_json: str) -> List[Dict[str, Any]]:
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"--stories must be valid JSON: {exc}")

    candidate = parsed.get("stories") if isinstance(parsed, dict) else parsed
    if not isinstance(candidate, list):
        raise typer.BadParameter(
            "--stories must be a JSON list or an object with a 'stories' list."
        )

    normalized: List[Dict[str, Any]] = []
    for idx, item in enumerate(candidate):
        if isinstance(item, str) and item.strip():
            normalized.append(_normalize_inline_story({"title": item.strip()}, index=idx))
            continue
        if isinstance(item, dict):
            normalized.append(_normalize_inline_story(item, index=idx))
            continue
    if not normalized:
        raise typer.BadParameter("--stories does not contain any valid story input.")
    return normalized


def _coerce_score(value: Any) -> Optional[float]:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return None
    return score


def _render_story_casting_details(
    *,
    stories: List[Dict[str, Any]],
    castings: List[Dict[str, Any]],
    strategy: Optional[Dict[str, Any]],
) -> None:
    if not castings:
        return

    story_by_id: Dict[str, Dict[str, Any]] = {}
    for story in stories:
        if not isinstance(story, dict):
            continue
        story_id = _compact_text(story.get("id"))
        if story_id:
            story_by_id[story_id] = story

    fallback_note = _compact_text((strategy or {}).get("fallbackNote"))

    console.print("\n[bold]Story Casting[/bold]")
    if fallback_note:
        console.print("[yellow]fallback used[/yellow]")

    for row in castings:
        if not isinstance(row, dict):
            continue

        story_id = _compact_text(row.get("storyId"))
        story = story_by_id.get(story_id, {})
        title = _compact_text(story.get("title")) or story_id or "Untitled story"
        test_focus = _compact_text(story.get("testFocus"))
        narrative = _compact_text(story.get("narrative"))

        status = _compact_text(row.get("status")).lower()
        status_display = status or "unknown"
        status_style = "green" if status == "matched" else "yellow"

        console.print(f"\n[bold]{title}[/bold]")
        console.print(f"[{status_style}]{status_display}[/{status_style}]")
        if test_focus:
            console.print(test_focus)
        if narrative:
            console.print(narrative)

        if status == "no_match":
            reason_code = _compact_text(row.get("reasonCode")).upper()
            message = _compact_text(row.get("message")) or _compact_text(row.get("detailReason"))
            if reason_code and message:
                console.print(f"{reason_code}: {message}")
            elif reason_code:
                console.print(reason_code)
            elif message:
                console.print(message)

            best_effort = row.get("bestEffort")
            if isinstance(best_effort, dict):
                persona_name = _compact_text(best_effort.get("personaName")) or _compact_text(
                    best_effort.get("personaId")
                )
                score = _coerce_score(best_effort.get("score"))
                if persona_name and score is not None:
                    console.print(f"Best-effort: {persona_name} ({score:.4f})")
                elif persona_name:
                    console.print(f"Best-effort: {persona_name}")
            continue

        if status == "matched":
            match_reason = _compact_text(row.get("matchReason")) or _compact_text(row.get("message"))
            if match_reason:
                console.print(match_reason)

    coverage_note = _compact_text((strategy or {}).get("coverageNote"))
    diversity_note = _compact_text((strategy or {}).get("diversityNote"))
    if coverage_note or diversity_note or fallback_note:
        console.print()
        if coverage_note:
            console.print(f"Coverage: {coverage_note}")
        if diversity_note:
            console.print(f"Diversity: {diversity_note}")
        if fallback_note:
            console.print(f"Fallback: {fallback_note}")


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
    stories_file: Optional[Path] = typer.Option(
        None,
        "--stories-file",
        help=(
            "Path to JSON/YAML stories for cast-only mode. "
            "Accepts a list of stories or {\"stories\": [...]}."
        ),
    ),
    stories: Optional[str] = typer.Option(
        None,
        "--stories",
        help=(
            "Inline JSON stories for cast-only mode. "
            "Accepts a JSON list of stories or {\"stories\": [...]}."
        ),
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

    if stories_file:
        payload["stories"] = _load_stories_from_file(stories_file)
        console.print(
            f"[dim]Using external stories ({len(payload['stories'])}) from {stories_file}[/dim]"
        )

    if stories:
        payload["stories"] = _parse_inline_stories(stories)
        console.print(
            f"[dim]Using inline stories ({len(payload['stories'])}) from --stories[/dim]"
        )

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
        stories = data.get("stories", []) if isinstance(data, dict) else []
        castings = data.get("castings", []) if isinstance(data, dict) else []
        strategy = data.get("strategy") if isinstance(data, dict) else None

        if not isinstance(personas, list):
            personas = []
        if not isinstance(stories, list):
            stories = []
        if not isinstance(castings, list):
            castings = []
        if not isinstance(strategy, dict):
            strategy = None

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
        if personas:
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
        else:
            console.print("[yellow]No personas suggested.[/yellow]")

        _render_story_casting_details(
            stories=stories,
            castings=castings,
            strategy=strategy,
        )

        # Save to cache
        cache_path = save_cache_file(
            "personas",
            f"suggested_{scenario_id}.yaml",
            {
                "persona_ids": suggested_ids,
                "personas": personas,
                "stories": stories if isinstance(stories, list) else [],
                "castings": castings if isinstance(castings, list) else [],
            },
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

        if format == "json":
            import json

            console.print_json(json.dumps(personas, ensure_ascii=False, default=str))
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
