"""
Unified progress / spinner utilities for FluxLoop CLI.

Two public APIs:
- spin_while(label, fn)       -- wrap a blocking call with a spinner + elapsed time
- SpinnerStatus context mgr   -- wrap a polling loop with live status updates
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any, Callable, Optional, TypeVar

from rich.console import Console
from rich.live import Live
from rich.spinner import Spinner
from rich.text import Text

T = TypeVar("T")


def _is_interactive() -> bool:
    """Return True when stdout is a real TTY and not an agent/CI environment."""
    if not sys.stdout.isatty():
        return False
    if os.getenv("CLAUDE_CODE") or os.getenv("CURSOR_AGENT"):
        return False
    if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
        return False
    return True


class _ElapsedSpinner:
    """Rich renderable: spinner + label + elapsed time, auto-updated on each refresh."""

    def __init__(self, label: str, spinner_name: str = "dots") -> None:
        self._label = label
        self._spinner = Spinner(spinner_name)
        self._start = time.monotonic()

    @property
    def label(self) -> str:
        return self._label

    @label.setter
    def label(self, value: str) -> None:
        self._label = value

    def __rich_console__(self, console: Console, options: Any):  # noqa: ANN401
        elapsed = int(time.monotonic() - self._start)
        text = Text.assemble(
            self._label,
            " ",
            Text(f"({elapsed}s)", style="dim"),
        )
        self._spinner.text = text
        yield self._spinner


def spin_while(
    label: str,
    fn: Callable[[], T],
    *,
    console: Optional[Console] = None,
) -> T:
    """
    Execute *fn* while showing a spinner with elapsed time.

    Non-interactive environments get a static message instead.
    Exceptions from *fn* propagate normally (spinner is cleaned up first).
    """
    console = console or Console()

    if not _is_interactive():
        console.print(f"[cyan]{label}[/cyan]")
        return fn()

    renderable = _ElapsedSpinner(label)
    with Live(renderable, console=console, refresh_per_second=4):
        return fn()


class SpinnerStatus:
    """
    Context manager for polling loops that need live status updates.

    Usage::

        with SpinnerStatus("Waiting for evaluation...", console=console) as status:
            while not done:
                status.update("running (3/10)")
                time.sleep(poll_interval)

    Non-interactive mode: update() prints a line only when text changes.
    """

    def __init__(
        self,
        label: str,
        *,
        console: Optional[Console] = None,
        spinner_name: str = "dots",
    ) -> None:
        self._console = console or Console()
        self._interactive = _is_interactive()
        self._label = label
        self._spinner_name = spinner_name
        self._renderable: Optional[_ElapsedSpinner] = None
        self._live: Optional[Live] = None
        self._last_printed: Optional[str] = None

    def __enter__(self) -> SpinnerStatus:
        if self._interactive:
            self._renderable = _ElapsedSpinner(self._label, self._spinner_name)
            self._live = Live(
                self._renderable,
                console=self._console,
                refresh_per_second=4,
            )
            self._live.__enter__()
        else:
            self._console.print(f"[cyan]{self._label}[/cyan]")
        return self

    def update(self, text: str) -> None:
        """Update the status text shown next to the spinner."""
        if self._interactive and self._renderable is not None:
            self._renderable.label = text
        else:
            if text != self._last_printed:
                self._console.print(f"  {text}")
                self._last_printed = text

    def __exit__(self, *exc_info: Any) -> None:
        if self._live is not None:
            self._live.__exit__(*exc_info)
