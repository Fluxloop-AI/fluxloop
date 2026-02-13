"""
Authentication commands for FluxLoop CLI.

Designed for both interactive and agent (Claude Code, Cursor) environments.
"""

import os
import sys
import webbrowser
from datetime import datetime, timezone
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.spinner import Spinner
from rich.live import Live

from ..api_utils import resolve_api_url
from ..auth_manager import (
    start_device_code_flow,
    poll_device_code,
    save_auth_token,
    load_auth_token,
    delete_auth_token,
    save_pending_login,
    load_pending_login,
    delete_pending_login,
    is_token_expired,
    ensure_valid_token,
    format_expires_at,
)


app = typer.Typer(help="Manage FluxLoop authentication")
console = Console()


def _is_agent_environment() -> bool:
    """Detect if running in an agent/headless environment."""
    # Check common agent environment indicators
    if not sys.stdout.isatty():
        return True
    if os.getenv("CLAUDE_CODE") or os.getenv("CURSOR_AGENT"):
        return True
    if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
        return True
    return False


@app.command()
def login(
    api_url: Optional[str] = typer.Option(
        None, "--api-url", help="FluxLoop API base URL"
    ),
    staging: bool = typer.Option(
        False, "--staging", help="Use staging API (staging.api.fluxloop.ai)"
    ),
    force: bool = typer.Option(
        False, "--force", "-f", help="Force re-login even if already logged in"
    ),
    no_browser: bool = typer.Option(
        False, "--no-browser", help="Don't open browser automatically (for CI/CD or headless environments)"
    ),
    no_wait: bool = typer.Option(
        False,
        "--no-wait",
        help="Print device code and exit without polling (use --resume to finish)",
    ),
    resume: bool = typer.Option(
        False,
        "--resume",
        help="Resume a pending login by polling saved device code",
    ),
):
    """
    Login to connect your FluxLoop account.
    
    If already logged in with a valid token, this command will skip login
    unless --force is specified. This makes the command idempotent and
    safe to call from automated scripts or AI agents.
    """
    api_url = resolve_api_url(api_url, staging=staging)

    # Check if already logged in (idempotent behavior)
    if not force:
        existing_token = load_auth_token()
        if existing_token and not is_token_expired(existing_token):
            console.print(f"[green]✓[/green] Already logged in: [bold]{existing_token.user_email}[/bold]")
            time_remaining = format_expires_at(existing_token.expires_at)
            console.print(f"  Token expires: {time_remaining}")
            console.print("[dim]Use --force to re-login with a new session.[/dim]")
            return

    if resume:
        pending = load_pending_login()
        if not pending:
            console.print("[red]✗[/red] No pending login found. Run 'fluxloop auth login --no-wait' first.")
            print("LOGIN_STATUS: no_pending")
            raise typer.Exit(1)

        pending_api_url = pending.get("api_url") or api_url
        device_code = pending.get("device_code")
        if not device_code:
            console.print("[red]✗[/red] Pending login data is missing device_code.")
            delete_pending_login()
            print("LOGIN_STATUS: invalid_pending")
            raise typer.Exit(1)

        interval = pending.get("interval") or 5
        timeout = pending.get("expires_in") or 300
        expires_at = pending.get("expires_at")
        if expires_at:
            try:
                expires_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
                if expires_dt.tzinfo is None:
                    expires_dt = expires_dt.replace(tzinfo=timezone.utc)
                remaining = int((expires_dt - datetime.now(timezone.utc)).total_seconds())
                if remaining <= 0:
                    delete_pending_login()
                    console.print("[red]✗[/red] Pending login has expired. Run 'fluxloop auth login --no-wait' again.")
                    print("LOGIN_STATUS: expired_pending")
                    raise typer.Exit(1)
                timeout = remaining
            except ValueError:
                pass

        try:
            # Poll for completion
            token = poll_device_code(
                pending_api_url,
                device_code,
                interval=interval,
                timeout=timeout,
            )

            save_auth_token(token)
            delete_pending_login()

            print()
            print(f"LOGIN_SUCCESS: {token.user_email}")
            console.print(f"[green]✓[/green] Login successful: [bold]{token.user_email}[/bold]")
            console.print()
            console.print("[dim]Next steps:[/dim]")
            console.print("[dim]  • fluxloop projects list    - View your projects[/dim]")
            console.print("[dim]  • fluxloop context set-project <id>  - Select a project[/dim]")
            return
        except TimeoutError as e:
            delete_pending_login()
            console.print(f"\n[red]✗[/red] {e}", style="bold red")
            raise typer.Exit(1)
        except ValueError as e:
            delete_pending_login()
            console.print(f"\n[red]✗[/red] {e}", style="bold red")
            raise typer.Exit(1)
        except Exception as e:
            delete_pending_login()
            console.print(f"\n[red]✗[/red] Login failed: {e}", style="bold red")
            raise typer.Exit(1)

    # Detect agent environment for adjusted behavior
    is_agent = _is_agent_environment()
    skip_browser = no_browser

    try:
        # Start device-code flow
        device_response = start_device_code_flow(api_url)

        # Always print plain text for agent compatibility (parseable output)
        # Use flush=True to ensure output appears immediately in agent environments
        print(flush=True)
        print("=" * 50, flush=True)
        print("FLUXLOOP LOGIN", flush=True)
        print("=" * 50, flush=True)
        print(f"URL:  {device_response.verification_url}", flush=True)
        print(f"CODE: {device_response.user_code}", flush=True)
        print("=" * 50, flush=True)
        print(flush=True)

        # Also display rich panel for interactive terminals
        if sys.stdout.isatty():
            login_panel = Panel(
                f"[bold cyan]Open this URL in your browser:[/bold cyan]\n"
                f"{device_response.verification_url}\n\n"
                f"[bold yellow]Enter code:[/bold yellow] [bold green]{device_response.user_code}[/bold green]",
                title="[bold blue]FluxLoop CLI Login[/bold blue]",
                border_style="blue",
            )
            console.print(login_panel)
            console.print()

        # Save pending login info for agent/non-blocking resume
        pending_path = save_pending_login(device_response, api_url)

        # Try to open browser automatically (unless disabled with --no-browser)
        if not skip_browser:
            try:
                webbrowser.open(device_response.verification_url)
                console.print("[dim]Browser opened automatically...[/dim]")
            except Exception:
                console.print("[dim]Please open the browser manually.[/dim]")
        else:
            print("(Browser auto-open disabled with --no-browser)", flush=True)

        print(flush=True)
        print("Waiting for approval...", flush=True)

        if no_wait:
            print(f"PENDING_LOGIN_PATH: {pending_path}", flush=True)
            console.print("[yellow]Login pending.[/yellow] Open the URL, enter the code, then run:")
            console.print("[dim]  fluxloop auth login --resume[/dim]")
            return

        # Poll for completion
        if sys.stdout.isatty() and not is_agent:
            # Use spinner for interactive terminals
            spinner = Spinner("dots", text="Waiting for approval...")
            with Live(spinner, console=console, refresh_per_second=10):
                token = poll_device_code(
                    api_url,
                    device_response.device_code,
                    interval=device_response.interval,
                    timeout=device_response.expires_in,
                )
        else:
            # Simple polling for agents (no spinner)
            token = poll_device_code(
                api_url,
                device_response.device_code,
                interval=device_response.interval,
                timeout=device_response.expires_in,
            )

        # Save token
        save_auth_token(token)
        delete_pending_login()

        # Success message (User-scoped, no project selection at login)
        print()
        print(f"LOGIN_SUCCESS: {token.user_email}")
        console.print(f"[green]✓[/green] Login successful: [bold]{token.user_email}[/bold]")
        console.print()
        console.print("[dim]Next steps:[/dim]")
        console.print("[dim]  • fluxloop projects list    - View your projects[/dim]")
        console.print("[dim]  • fluxloop context set-project <id>  - Select a project[/dim]")

    except TimeoutError as e:
        delete_pending_login()
        console.print(f"\n[red]✗[/red] {e}", style="bold red")
        raise typer.Exit(1)
    except ValueError as e:
        delete_pending_login()
        console.print(f"\n[red]✗[/red] {e}", style="bold red")
        raise typer.Exit(1)
    except Exception as e:
        delete_pending_login()
        console.print(f"\n[red]✗[/red] Login failed: {e}", style="bold red")
        raise typer.Exit(1)


@app.command()
def logout():
    """
    Logout and delete saved authentication information.
    """
    token = load_auth_token()

    if not token:
        console.print("[yellow]Already logged out.[/yellow]")
        print("LOGOUT_STATUS: already_logged_out")
        return

    delete_auth_token()
    console.print("[green]✓[/green] Logged out successfully")
    print("LOGOUT_STATUS: success")


@app.command()
def status():
    """
    Check current login status.
    
    If token is expired or about to expire, automatically attempts to refresh.
    Only suggests re-login if refresh fails.
    """
    api_url = resolve_api_url()
    token = load_auth_token()

    if not token:
        console.print("[yellow]Not logged in.[/yellow]")
        console.print("[dim]Run [bold]fluxloop auth login[/bold] to login.[/dim]")
        print("AUTH_STATUS: not_logged_in")
        return

    # Check expiration - if expired, try refresh first
    expired = is_token_expired(token)
    
    if expired:
        # Try to refresh before declaring "expired"
        console.print("[dim]Token expired or expiring soon, attempting refresh...[/dim]")
        refreshed_token = ensure_valid_token(api_url)
        
        if refreshed_token:
            # Refresh succeeded
            token = refreshed_token
            time_remaining = format_expires_at(token.expires_at)
            console.print(f"[green]✓[/green] Logged in: [bold]{token.user_email}[/bold] (token refreshed)")
            console.print(f"  Token expires: {time_remaining}")
            print(f"AUTH_STATUS: logged_in (refreshed)")
            print(f"AUTH_USER: {token.user_email}")
        else:
            # Refresh failed
            console.print("[red]✗[/red] Token expired and refresh failed")
            console.print("[dim]Run [bold]fluxloop auth login[/bold] to login again.[/dim]")
            print("AUTH_STATUS: expired")
            return
    else:
        time_remaining = format_expires_at(token.expires_at)
        console.print(f"[green]✓[/green] Logged in: [bold]{token.user_email}[/bold]")
        console.print(f"  Token expires: {time_remaining}")
        print(f"AUTH_STATUS: logged_in")
        print(f"AUTH_USER: {token.user_email}")
    
    # Show current context if available
    from ..context_manager import load_context, load_project_connection
    
    project_conn = load_project_connection()
    if project_conn:
        console.print(f"  Project: {project_conn.project_name} ([dim]{project_conn.project_id}[/dim])")
        print(f"AUTH_PROJECT: {project_conn.project_id}")
    
    context = load_context()
    if context and context.current_scenario:
        console.print(f"  Scenario: {context.current_scenario.name} ([dim]{context.current_scenario.id}[/dim])")
        print(f"AUTH_SCENARIO: {context.current_scenario.id}")
    
    if not project_conn:
        console.print("[dim]  No project selected. Run [bold]fluxloop projects select[/bold] to choose a project.[/dim]")