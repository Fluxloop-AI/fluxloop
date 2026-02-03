"""
Conversation supervisor for multi-turn simulations.

This module encapsulates the logic required to consult an LLM (or mock strategy)
to decide whether a conversation should continue and, if so, to generate the
next user utterance consistent with the experiment persona and service context.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Sequence

import httpx

from fluxloop.schemas import MultiTurnSupervisorConfig

logger = logging.getLogger(__name__)
SupervisorDecisionType = Literal["continue", "terminate"]


@dataclass
class SupervisorDecision:
    """Structured decision returned by the conversation supervisor."""

    decision: SupervisorDecisionType
    next_user_message: Optional[str] = None
    termination_reason: Optional[str] = None
    closing_user_message: Optional[str] = None
    raw_response: Optional[Dict[str, Any]] = None

    def requires_follow_up(self) -> bool:
        return self.decision == "continue"


def _coerce_text(value: Any) -> str:
    """Convert any value into a safe string representation."""

    if value is None:
        return ""

    if isinstance(value, str):
        return value

    if isinstance(value, (dict, list, tuple, set)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)

    return str(value)


def format_transcript(turns: Sequence[Dict[str, Any]]) -> str:
    """Render a conversation transcript into a bullet list for supervisor prompts."""

    lines: List[str] = []
    for idx, turn in enumerate(turns, start=1):
        role = turn.get("role", "unknown").capitalize()
        content = _coerce_text(turn.get("content"))
        content = content.strip()

        if turn.get("tool_calls"):
            tool_calls = turn["tool_calls"]
            if isinstance(tool_calls, list):
                tool_summaries = []
                for call in tool_calls:
                    name = call.get("name") or call.get("tool") or "tool"
                    args = call.get("arguments") or call.get("args") or {}
                    tool_summaries.append(f"{name}({_coerce_text(args)})")
                if tool_summaries:
                    content = f"{content}\n    [Tool Calls] " + "; ".join(tool_summaries)
        lines.append(f"{idx}. {role}: {content}")

    return "\n".join(lines)


def build_supervisor_prompt(
    turns: Sequence[Dict[str, Any]],
    persona_description: Optional[str],
    service_context: Optional[str],
    instructions: Optional[str],
) -> str:
    """Create the textual prompt sent to the supervisor LLM."""

    persona_text = persona_description or "Generic customer"
    service_text = service_context or "Customer support scenario"
    transcript_text = format_transcript(turns)

    guidance = instructions or (
        "You supervise an AI assistant. Review the transcript and decide whether the "
        "conversation should continue. When continuing, craft the next user message "
        "consistent with the persona. When terminating, explain why and provide any "
        "closing notes."
    )

    prompt = (
        f"{guidance}\n\n"
        f"Service Context: {service_text}\n"
        f"Persona: {persona_text}\n\n"
        "Transcript so far:\n"
        f"{transcript_text}\n\n"
        "Respond ONLY in JSON with the following schema:\n"
        "{\n"
        '  "decision": "continue" | "terminate",\n'
        '  "next_user_message": string | null,\n'
        '  "termination_reason": string | null,\n'
        '  "closing_user_message": string | null\n'
        "}\n"
    )

    return prompt


class ConversationSupervisor:
    """High-level interface for querying the conversation supervisor LLM."""

    def __init__(self, config: MultiTurnSupervisorConfig):
        self.config = config

    async def decide(
        self,
        *,
        conversation_state: Dict[str, Any],
        persona_description: Optional[str],
        service_context: Optional[str],
    ) -> SupervisorDecision:
        turns: Sequence[Dict[str, Any]] = conversation_state.get("turns", [])
        prompt = build_supervisor_prompt(
            turns=turns,
            persona_description=persona_description,
            service_context=service_context,
            instructions=self.config.system_prompt,
        )

        provider = (self.config.provider or "openai").lower()
        logger.debug(
            "supervisor.decide provider=%s turns=%d persona=%r service=%r",
            provider,
            len(turns),
            persona_description,
            service_context,
        )
        if provider == "mock":
            return self._mock_decision(conversation_state)
        if provider == "openai":
            response_text = await self._call_openai(prompt)
        elif provider == "anthropic":
            response_text = await self._call_anthropic(prompt)
        else:
            raise ValueError(f"Unsupported supervisor provider: {self.config.provider}")

        return self._parse_decision(response_text)

    def _mock_decision(self, conversation_state: Dict[str, Any]) -> SupervisorDecision:
        """Deterministic supervisor used for tests, scripted runs, or offline modes."""

        metadata = self.config.metadata or {}
        logger.debug(
            "mock supervisor state: scripted_questions=%s context_keys=%s",
            isinstance(metadata.get("scripted_questions"), list),
            list(conversation_state.get("context", {}).keys()),
        )

        scripted_questions = metadata.get("scripted_questions")
        if isinstance(scripted_questions, list):
            context = conversation_state.setdefault("context", {})
            index = int(context.get("mock_script_index", 0))
            script_length = len(scripted_questions)

            if index < script_length:
                next_message = scripted_questions[index]
                context["mock_script_index"] = index + 1
                return SupervisorDecision(
                    decision="continue",
                    next_user_message=str(next_message),
                    raw_response={
                        "mock": True,
                        "scripted": True,
                        "script_index": index,
                        "script_length": script_length,
                    },
                )

            reason = metadata.get("mock_reason", "script_complete")
            closing = metadata.get("mock_closing")
            return SupervisorDecision(
                decision="terminate",
                termination_reason=reason,
                closing_user_message=closing,
                raw_response={
                    "mock": True,
                    "scripted": True,
                    "script_complete": True,
                    "script_length": script_length,
                },
            )

        default_decision = metadata.get("mock_decision", "terminate")
        decision: SupervisorDecisionType = (
            default_decision if default_decision in {"continue", "terminate"} else "terminate"
        )

        if decision == "continue":
            next_message = metadata.get(
                "mock_next_user_message",
                "Could you provide a bit more detail about that?",
            )
            return SupervisorDecision(
                decision="continue",
                next_user_message=next_message,
                raw_response={"mock": True},
            )

        return SupervisorDecision(
            decision="terminate",
            termination_reason=metadata.get("mock_reason", "Supervisor mock termination"),
            closing_user_message=metadata.get(
                "mock_closing",
                "Thanks for the help. I have no further questions.",
            ),
            raw_response={"mock": True},
        )

    async def _call_openai(self, prompt: str) -> str:
        """Call OpenAI chat completions endpoint."""

        api_key = (
            self.config.api_key
            or self.config.metadata.get("api_key")
            or os.getenv("FLUXLOOP_SUPERVISOR_API_KEY")
            or os.getenv("OPENAI_API_KEY")
        )

        if not api_key:
            raise RuntimeError(
                "OpenAI supervisor requires an API key. "
                "Set multi_turn.supervisor.api_key or FLUXLOOP_SUPERVISOR_API_KEY/OPENAI_API_KEY."
            )

        messages: List[Dict[str, str]] = []
        if self.config.system_prompt:
            messages.append({"role": "system", "content": self.config.system_prompt})
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": self.config.model,
            "messages": messages,
        }
        if self.config.temperature is not None:
            payload["temperature"] = self.config.temperature

        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )

        if response.status_code >= 400:
            raise RuntimeError(
                f"OpenAI supervisor request failed ({response.status_code}): {response.text}"
            )

        data = response.json()
        choices = data.get("choices") or []
        if not choices:
            raise RuntimeError("OpenAI supervisor response contained no choices.")

        message = choices[0].get("message") or {}
        content = message.get("content")
        if not content:
            raise RuntimeError("OpenAI supervisor response missing content.")

        return content

    async def _call_anthropic(self, prompt: str) -> str:
        """Call Anthropic messages endpoint."""

        api_key = (
            self.config.api_key
            or self.config.metadata.get("api_key")
            or os.getenv("FLUXLOOP_SUPERVISOR_API_KEY")
            or os.getenv("ANTHROPIC_API_KEY")
        )

        if not api_key:
            raise RuntimeError(
                "Anthropic supervisor requires an API key. "
                "Set multi_turn.supervisor.api_key or FLUXLOOP_SUPERVISOR_API_KEY/ANTHROPIC_API_KEY."
            )

        messages: List[Dict[str, str]] = [{"role": "user", "content": prompt}]

        payload: Dict[str, Any] = {
            "model": self.config.model or "claude-sonnet-4-5-20250514",
            "max_tokens": 1024,
            "messages": messages,
        }
        if self.config.system_prompt:
            payload["system"] = self.config.system_prompt
        if self.config.temperature is not None:
            payload["temperature"] = self.config.temperature

        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "Content-Type": "application/json",
                },
                json=payload,
            )

        if response.status_code >= 400:
            raise RuntimeError(
                f"Anthropic supervisor request failed ({response.status_code}): {response.text}"
            )

        data = response.json()
        content_blocks = data.get("content") or []
        if not content_blocks:
            raise RuntimeError("Anthropic supervisor response contained no content.")

        # Extract text from content blocks
        text_parts = []
        for block in content_blocks:
            if block.get("type") == "text":
                text_parts.append(block.get("text", ""))

        content = "".join(text_parts)
        if not content:
            raise RuntimeError("Anthropic supervisor response missing text content.")

        return content

    def _parse_decision(self, content: str) -> SupervisorDecision:
        """Extract JSON payload from supervisor response and convert to decision."""

        payload = self._extract_json(content)
        decision_value = payload.get("decision")
        if decision_value not in {"continue", "terminate"}:
            raise ValueError(f"Supervisor returned invalid decision: {decision_value}")

        decision: SupervisorDecisionType = decision_value  # type: ignore[assignment]
        result = SupervisorDecision(
            decision=decision,
            next_user_message=payload.get("next_user_message"),
            termination_reason=payload.get("termination_reason"),
            closing_user_message=payload.get("closing_user_message"),
            raw_response=payload,
        )

        if decision == "continue":
            if not (result.next_user_message and result.next_user_message.strip()):
                raise ValueError("Supervisor decided to continue but provided no next_user_message.")
        return result

    @staticmethod
    def _extract_json(content: str) -> Dict[str, Any]:
        """Attempt to parse a JSON object from the model output."""

        content = content.strip()
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass

        # Attempt to find JSON substring
        match = re.search(r"\{.*\}", content, flags=re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass

        raise ValueError(f"Supervisor response was not valid JSON: {content}")


__all__ = ["ConversationSupervisor", "SupervisorDecision", "build_supervisor_prompt"]

