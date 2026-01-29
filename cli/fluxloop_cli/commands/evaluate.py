"""
Trigger server-side evaluation for an experiment.
"""

from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import List, Optional

import typer
from rich.console import Console

from ..api_utils import handle_api_error, resolve_api_url
from ..context_manager import get_current_web_project_id
from ..http_client import create_authenticated_client, post_with_retry


app = typer.Typer(help="Trigger evaluation for an experiment (server-side).")
console = Console()

def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except ValueError:
        return None


@app.callback(invoke_without_command=True)
def main(
    experiment_id: str = typer.Option(..., "--experiment-id", help="Experiment ID"),
    project_id: Optional[str] = typer.Option(
        None, "--project-id", help="Project ID (auto-detected from context)"
    ),
    config_id: Optional[str] = typer.Option(
        None, "--config-id", help="Evaluation config ID (optional)"
    ),
    run_ids: Optional[List[str]] = typer.Option(
        None, "--run-id", help="Specific run IDs to evaluate (repeatable)"
    ),
    force_rerun: bool = typer.Option(
        False, "--force-rerun", help="Force re-run even if job exists"
    ),
    wait: bool = typer.Option(
        False, "--wait", help="Wait for the evaluation job to complete"
    ),
    timeout: int = typer.Option(
        600, "--timeout", help="Max seconds to wait for completion"
    ),
    poll_interval: int = typer.Option(
        3, "--poll-interval", help="Polling interval in seconds"
    ),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
):
    """
    Trigger server-side evaluation for an experiment.

    Uses the current logged-in user (JWT). If no config is provided, the server
    will auto-select or use scenario defaults.
    """
    api_url = resolve_api_url(api_url)

    if not project_id:
        project_id = get_current_web_project_id()
        if not project_id:
            console.print("[yellow]No Web Project selected.[/yellow]")
            console.print("[dim]Select one with: fluxloop projects select <id>[/dim]")
            raise typer.Exit(1)

    payload = {
        "project_id": project_id,
        "experiment_id": experiment_id,
        "config_id": config_id,
        "run_ids": run_ids,
        "force_rerun": force_rerun,
        "source": "cli",
    }
    payload = {k: v for k, v in payload.items() if v is not None}

    client = create_authenticated_client(api_url, use_jwt=True)
    resp = post_with_retry(client, "/api/evaluations", payload=payload)
    handle_api_error(resp, "evaluation")
    result = resp.json()

    console.print("[green]✓[/green] Evaluation triggered")
    console.print(f"  id: {result.get('evaluation_id')}")
    console.print(f"  status: {result.get('status')}")

    if not wait:
        return

    evaluation_id = result.get("evaluation_id")
    if not evaluation_id:
        console.print("[red]✗[/red] Missing evaluation_id in response.")
        raise typer.Exit(1)

    console.print("[dim]Waiting for evaluation job to complete...[/dim]")
    start = time.monotonic()
    warned = False
    last_status = None

    while True:
        if time.monotonic() - start > timeout:
            console.print("[red]✗[/red] Timed out waiting for evaluation job.")
            raise typer.Exit(1)

        resp = client.get(
            f"/api/experiments/{experiment_id}/evaluations",
            params={"project_id": project_id},
        )
        handle_api_error(resp, "evaluation jobs")
        jobs = resp.json()
        job = next((j for j in jobs if j.get("id") == evaluation_id), None)

        if not job:
            if last_status != "missing":
                console.print("[yellow]⚠[/yellow] Evaluation job not visible yet. Retrying...")
                last_status = "missing"
            time.sleep(poll_interval)
            continue

        status = job.get("status") or "queued"
        progress = job.get("progress") or {}
        if status != last_status:
            status_line = f"  status: {status}"
            total = progress.get("total")
            completed = progress.get("completed")
            failed = progress.get("failed")
            if total is not None:
                status_line += f" ({completed or 0}/{total}"
                if failed:
                    status_line += f", failed {failed}"
                status_line += ")"
            console.print(status_line)
            last_status = status

        if status in ("completed", "partial", "failed", "cancelled"):
            break

        if status == "queued" and not warned:
            created_at = _parse_iso_datetime(job.get("created_at"))
            locked_at = _parse_iso_datetime(job.get("locked_at"))
            if created_at and not locked_at:
                age_seconds = (datetime.now(timezone.utc) - created_at).total_seconds()
                if age_seconds > 30:
                    console.print(
                        "[yellow]⚠[/yellow] Evaluation job still queued. "
                        "Worker may not be running or backlog is high."
                    )
                    warned = True

        time.sleep(poll_interval)

    if status in ("completed", "partial"):
        insights_resp = client.get(
            f"/api/experiments/{experiment_id}/insights",
            params={"project_id": project_id},
        )
        handle_api_error(insights_resp, "insights")
        insights = insights_resp.json()
        insight_headline = None
        if insights:
            content = insights[0].get("content") or {}
            summary = content.get("summary") or {}
            insight_headline = summary.get("headline")

        recos_resp = client.get(
            f"/api/experiments/{experiment_id}/recommendations",
            params={"project_id": project_id},
        )
        handle_api_error(recos_resp, "recommendations")
        recos = recos_resp.json()
        reco_headline = None
        if recos:
            content = recos[0].get("content") or {}
            summary = content.get("summary") or {}
            reco_headline = summary.get("headline")

        if insight_headline:
            console.print(f"[green]Insights[/green]: {insight_headline}")
        if reco_headline:
            console.print(f"[green]Recommendations[/green]: {reco_headline}")
