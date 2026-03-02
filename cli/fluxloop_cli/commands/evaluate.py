"""
Trigger server-side evaluation for an experiment.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
import time
from typing import Any, List, Optional

import typer
from rich.console import Console

from ..api_utils import handle_api_error, resolve_api_url
from ..context_manager import get_current_web_project_id
from ..http_client import create_authenticated_client, post_with_retry
from ..progress import SpinnerStatus


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


def _decision_is_empty(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return True
    for key in ("release_decision", "decision_snapshot", "gate_snapshot", "gate_results_snapshot"):
        if payload.get(key) is not None:
            return False
    return True


def _normalize_gate_reason(gate_result: dict[str, Any]) -> Optional[str]:
    raw_reasons = gate_result.get("reasons")
    if isinstance(raw_reasons, list):
        tokens = [str(item).strip() for item in raw_reasons if str(item).strip()]
    else:
        raw_reason = gate_result.get("reason")
        if raw_reason is None:
            return None
        tokens = [
            item.strip()
            for item in str(raw_reason).replace(";", ",").split(",")
            if item.strip()
        ]

    normalized: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        clean = token
        if ":" in token:
            prefix, suffix = token.rsplit(":", 1)
            if suffix.strip().isdigit():
                clean = prefix.strip()
        if clean and clean not in seen:
            seen.add(clean)
            normalized.append(clean)
    if not normalized:
        return None
    return ", ".join(normalized)


def _format_budget_value(key: str, value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if key == "cost_usd":
            return f"{value:.2f}"
        if value.is_integer():
            return str(int(value))
        return f"{value:.3f}".rstrip("0").rstrip(".")
    return str(value)


def _render_decision_text(payload: dict[str, Any]) -> str:
    decision_snapshot = payload.get("decision_snapshot")
    if not isinstance(decision_snapshot, dict):
        decision_snapshot = {}

    release_decision = payload.get("release_decision") or decision_snapshot.get("release_decision")
    overall_verdict = decision_snapshot.get("overall_verdict")

    gate_results = payload.get("gate_results_snapshot")
    if not isinstance(gate_results, list):
        snapshot_gate_results = decision_snapshot.get("gate_results")
        gate_results = snapshot_gate_results if isinstance(snapshot_gate_results, list) else []

    lines: list[str] = [
        f"Release Decision: {release_decision or 'unknown'}",
        f"Overall Verdict: {overall_verdict or 'unknown'}",
        "",
        "Gates:",
    ]

    if gate_results:
        for gate_result in gate_results:
            if not isinstance(gate_result, dict):
                continue
            gate_key = gate_result.get("gate_key") or gate_result.get("metric_key") or "unknown_gate"
            status = str(gate_result.get("status") or "unknown")
            line = f"  - {gate_key} => {status}"
            reason = _normalize_gate_reason(gate_result)
            if reason:
                line += f" ({reason})"
            lines.append(line)
    else:
        lines.append("  - (none)")

    metrics = decision_snapshot.get("metrics")
    lines.extend(["", "Budget:"])
    if isinstance(metrics, dict):
        budget_rows: list[str] = []
        for metric_key in ("tokens_used", "cost_usd", "latency_ms"):
            metric_value = metrics.get(metric_key)
            if metric_value is None:
                continue
            budget_rows.append(
                f"  - {metric_key}: {_format_budget_value(metric_key, metric_value)}"
            )
        if budget_rows:
            lines.extend(budget_rows)
        else:
            lines.append("  - N/A")
    else:
        lines.append("  - N/A")

    return "\n".join(lines)


def _show_decision(
    *,
    client,
    experiment_id: str,
    project_id: str,
    json_output: bool,
) -> None:
    decision_resp = client.get(
        f"/api/experiments/{experiment_id}/decision",
        params={"project_id": project_id},
    )
    if decision_resp.status_code == 404:
        console.print(
            f"[red]✗[/red] Decision not found for experiment: {experiment_id}"
        )
        raise typer.Exit(1)
    handle_api_error(decision_resp, "decision")

    decision_payload = decision_resp.json()
    if _decision_is_empty(decision_payload):
        console.print("[red]✗[/red] Decision is not available yet for this experiment.")
        console.print("[dim]Run with --wait and try again after evaluation completes.[/dim]")
        raise typer.Exit(1)

    if json_output:
        console.print_json(json.dumps(decision_payload, ensure_ascii=False, default=str))
        return

    console.print("")
    console.print(_render_decision_text(decision_payload))


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
    show_decision: bool = typer.Option(
        False, "--show-decision", help="Show release decision snapshot after evaluation"
    ),
    json_output: bool = typer.Option(
        False, "--json", help="Print decision response as raw JSON (with --show-decision)"
    ),
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
):
    """
    Trigger server-side evaluation for an experiment.

    Uses the current logged-in user (JWT). If no config is provided, the server
    will auto-select or use scenario defaults.
    """
    api_url = resolve_api_url(api_url, staging=staging)

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
        if show_decision:
            _show_decision(
                client=client,
                experiment_id=experiment_id,
                project_id=project_id,
                json_output=json_output,
            )
        elif json_output:
            console.print("[yellow]--json is ignored without --show-decision.[/yellow]")
        return

    evaluation_id = result.get("evaluation_id")
    if not evaluation_id:
        console.print("[red]✗[/red] Missing evaluation_id in response.")
        raise typer.Exit(1)

    start = time.monotonic()
    warned = False

    with SpinnerStatus("Waiting for evaluation job...", console=console) as spinner:
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
                spinner.update("Evaluation job not visible yet. Retrying...")
                time.sleep(poll_interval)
                continue

            status = job.get("status") or "queued"
            progress = job.get("progress") or {}

            status_line = status
            total = progress.get("total")
            completed = progress.get("completed")
            failed = progress.get("failed")
            if total is not None:
                status_line += f" ({completed or 0}/{total}"
                if failed:
                    status_line += f", failed {failed}"
                status_line += ")"

            spinner.update(status_line)

            if status in ("completed", "partial", "failed", "cancelled"):
                break

            if status == "queued" and not warned:
                created_at = _parse_iso_datetime(job.get("created_at"))
                locked_at = _parse_iso_datetime(job.get("locked_at"))
                if created_at and not locked_at:
                    age_seconds = (datetime.now(timezone.utc) - created_at).total_seconds()
                    if age_seconds > 30:
                        spinner.update("queued — Worker may not be running or backlog is high")
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

    if show_decision:
        _show_decision(
            client=client,
            experiment_id=experiment_id,
            project_id=project_id,
            json_output=json_output,
        )
    elif json_output:
        console.print("[yellow]--json is ignored without --show-decision.[/yellow]")
