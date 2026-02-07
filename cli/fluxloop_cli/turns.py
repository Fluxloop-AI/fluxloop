"""
Utilities for turn-level recording and result rendering.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml

from .constants import STATE_DIR_NAME


DEFAULT_FORBIDDEN_WORDS = ["죄송", "sorry"]
DEFAULT_MAX_RESPONSE_LENGTH = 2000


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_guardrails_from_config(config: Dict[str, Any]) -> Dict[str, Any]:
    guardrails = config.get("guardrails", {}) if isinstance(config, dict) else {}
    forbidden_words = guardrails.get("forbidden_words") or DEFAULT_FORBIDDEN_WORDS
    max_len = guardrails.get("max_response_length") or DEFAULT_MAX_RESPONSE_LENGTH
    return {
        "forbidden_words": [str(word) for word in forbidden_words],
        "max_response_length": int(max_len),
    }


def check_guardrails(content: str, guardrails: Dict[str, Any]) -> List[Dict[str, str]]:
    warnings: List[Dict[str, str]] = []
    text = content or ""

    if len(text.strip()) == 0:
        warnings.append({"type": "empty_response", "message": "빈 응답"})

    forbidden_words = guardrails.get("forbidden_words") or []
    for word in forbidden_words:
        if word and word in text:
            warnings.append({"type": "forbidden_words", "message": f"'{word}' 포함"})
            break

    max_len = guardrails.get("max_response_length") or DEFAULT_MAX_RESPONSE_LENGTH
    if len(text) > int(max_len):
        warnings.append({"type": "too_long", "message": "응답 길이 초과"})

    return warnings


def format_warning_for_display(warnings: List[Dict[str, str]]) -> Optional[str]:
    if not warnings:
        return None
    return warnings[0].get("message") or warnings[0].get("type")


def load_criteria_items(criteria_dir: Path) -> List[str]:
    if not criteria_dir.exists():
        return []
    items: List[str] = []
    for path in sorted(criteria_dir.glob("*.yaml")):
        payload = yaml.safe_load(path.read_text()) or {}
        if isinstance(payload, dict):
            for item in payload.get("items", []) or []:
                if isinstance(item, str):
                    items.append(item)
                elif isinstance(item, dict):
                    text = item.get("description") or item.get("text") or item.get("title")
                    if text:
                        items.append(str(text))
    return items


def load_contracts(contracts_dir: Path) -> List[Dict[str, Any]]:
    """Load contracts from YAML files in contracts directory.
    
    Contract structure:
    {
        "type": "must" | "should" | "must_not",
        "description": "...",
        "category": "response" | "safety" | "ux" | ...
    }
    
    Returns:
        List of contract dictionaries.
    """
    if not contracts_dir.exists():
        return []
    contracts: List[Dict[str, Any]] = []
    for path in sorted(contracts_dir.glob("*.yaml")):
        payload = yaml.safe_load(path.read_text()) or {}
        if isinstance(payload, dict):
            contracts.append(payload)
    return contracts


def _slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9_-]+", "_", value)
    return value.strip("_") or "criteria"


@dataclass
class TurnSummary:
    total_turns: int = 0
    warning_turns: int = 0
    warning_count: int = 0

    @property
    def warning_rate(self) -> float:
        return self.warning_turns / self.total_turns if self.total_turns else 0.0


class TurnRecorder:
    def __init__(self, turns_path: Path, guardrails: Dict[str, Any]) -> None:
        self.turns_path = turns_path
        self.turns_path.parent.mkdir(parents=True, exist_ok=True)
        self.guardrails = guardrails
        self._sequence_by_run: Dict[str, int] = defaultdict(int)
        self._assistant_turn_by_run: Dict[str, int] = defaultdict(int)
        self._summary_by_run: Dict[str, TurnSummary] = defaultdict(TurnSummary)
        self._turns_cache: List[Dict[str, Any]] = []

    def record_turn(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        run_id = payload["run_id"]
        self._sequence_by_run[run_id] += 1
        sequence = self._sequence_by_run[run_id]
        turn_id = f"{run_id}-t{sequence}"

        role = payload.get("role") or "assistant"
        content = payload.get("content") or ""
        warnings: List[Dict[str, str]] = []
        if role == "assistant":
            warnings = check_guardrails(content, self.guardrails)
            self._assistant_turn_by_run[run_id] += 1

        record = {
            "run_id": run_id,
            "turn_id": turn_id,
            "sequence": sequence,
            "role": role,
            "content": content,
            "timestamp": payload.get("timestamp") or utc_now_iso(),
        }
        if payload.get("duration_ms") is not None:
            try:
                record["duration_ms"] = int(payload["duration_ms"])
            except (TypeError, ValueError):
                pass
        if warnings:
            record["warnings"] = warnings

        self._append(record)
        self._turns_cache.append(record)
        self._update_summary(run_id, warnings)

        return record

    def _append(self, record: Dict[str, Any]) -> None:
        with self.turns_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _update_summary(self, run_id: str, warnings: List[Dict[str, str]]) -> None:
        summary = self._summary_by_run[run_id]
        summary.total_turns += 1
        if warnings:
            summary.warning_turns += 1
            summary.warning_count += len(warnings)

    def get_assistant_turn_count(self, run_id: str) -> int:
        return self._assistant_turn_by_run.get(run_id, 0)

    def get_run_summary(self, run_id: str) -> TurnSummary:
        return self._summary_by_run.get(run_id, TurnSummary())

    def get_overall_summary(self) -> TurnSummary:
        overall = TurnSummary()
        for summary in self._summary_by_run.values():
            overall.total_turns += summary.total_turns
            overall.warning_turns += summary.warning_turns
            overall.warning_count += summary.warning_count
        return overall

    def iter_turns(self) -> Iterable[Dict[str, Any]]:
        if self._turns_cache:
            return list(self._turns_cache)
        return load_turns(self.turns_path)


def load_turns(turns_path: Path) -> List[Dict[str, Any]]:
    turns: List[Dict[str, Any]] = []
    if not turns_path.exists():
        return turns
    with turns_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                turns.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return turns


def summarize_turns(turns: List[Dict[str, Any]]) -> Dict[str, TurnSummary]:
    summaries: Dict[str, TurnSummary] = defaultdict(TurnSummary)
    for turn in turns:
        run_id = turn.get("run_id")
        if not run_id:
            continue
        summary = summaries[run_id]
        summary.total_turns += 1
        warnings = turn.get("warnings") or []
        if warnings:
            summary.warning_turns += 1
            summary.warning_count += len(warnings)
    return summaries


def render_result_markdown(
    turns: List[Dict[str, Any]],
    summary: TurnSummary,
    criteria_items: List[str],
) -> str:
    lines: List[str] = []
    lines.append("# FluxLoop Test Result")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- Turns: {summary.total_turns} ({summary.warning_turns} warning turns)")
    lines.append(f"- Warnings: {summary.warning_count}")
    lines.append("")

    lines.append("## Turn Details")
    for turn in turns:
        lines.append("")
        lines.append(f"### Turn {turn.get('sequence')}")
        lines.append(f"- Role: {turn.get('role')}")
        if turn.get("duration_ms") is not None:
            lines.append(f"- Duration: {turn.get('duration_ms')} ms")
        warnings = turn.get("warnings") or []
        if warnings:
            lines.append("- Status: ⚠️ Warning")
        else:
            lines.append("- Status: ✓")
        lines.append("")
        lines.append("**Content:**")
        lines.append(f"> {turn.get('content')}")
        if warnings:
            lines.append("")
            lines.append("**Warnings:**")
            for warning in warnings:
                lines.append(f"- {warning.get('message') or warning.get('type')}")

    if criteria_items:
        lines.append("")
        lines.append("## Evaluation Criteria")
        for item in criteria_items:
            lines.append(f"- {item}")

    lines.append("")
    return "\n".join(lines)


def write_latest_result_link(project_root: Path, result_path: Path) -> Path:
    state_dir = project_root / STATE_DIR_NAME
    state_dir.mkdir(parents=True, exist_ok=True)
    latest_path = state_dir / "latest_result.md"
    try:
        if latest_path.exists() or latest_path.is_symlink():
            latest_path.unlink()
        latest_path.symlink_to(result_path)
    except OSError:
        latest_path.write_text(result_path.read_text(encoding="utf-8"), encoding="utf-8")
    return latest_path
