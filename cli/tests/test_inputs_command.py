from typer.testing import CliRunner

from fluxloop_cli.commands import inputs as inputs_cmd


runner = CliRunner()


class _FakeResponse:
    def __init__(self, payload, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self.is_success = 200 <= status_code < 300

    def json(self):
        return self._payload


def test_inputs_synthesize_shows_data_context_conflict(monkeypatch):
    captured = {}

    monkeypatch.setattr(
        inputs_cmd,
        "create_authenticated_client",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        inputs_cmd,
        "spin_while",
        lambda _message, fn, console=None: fn(),
    )
    monkeypatch.setattr(
        inputs_cmd,
        "post_with_retry",
        lambda _client, _path, payload: (
            captured.setdefault("payload", payload),
            _FakeResponse(
                {
                    "detail": "Data context is not ready.",
                    "code": "DATA_CONTEXT_NOT_READY",
                    "context": {
                        "total_count": 3,
                        "completed_count": 1,
                        "pending_data_ids": ["data_a", "data_b"],
                        "recommended_action": (
                            "Wait for data processing to complete, then regenerate summary."
                        ),
                    },
                },
                status_code=409,
            ),
        )[1],
    )
    monkeypatch.setattr(
        inputs_cmd,
        "handle_api_error",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected call")),
    )

    result = runner.invoke(
        inputs_cmd.app,
        [
            "synthesize",
            "--project-id",
            "project_1",
            "--scenario-id",
            "scenario_1",
            "--dry-run",
        ],
    )

    assert result.exit_code == 1
    assert captured["payload"]["include_data_context"] is True
    assert "Data context is not ready yet" in result.output
    assert "Completed: 1/3 | Pending: 2" in result.output
    assert "Retry command after data context is ready." in result.output


def test_inputs_refine_uses_context_project_id(monkeypatch):
    captured = {}

    monkeypatch.setattr(
        inputs_cmd,
        "get_current_web_project_id",
        lambda: "project_ctx",
    )
    monkeypatch.setattr(
        inputs_cmd,
        "create_authenticated_client",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        inputs_cmd,
        "spin_while",
        lambda _message, fn, console=None: fn(),
    )
    monkeypatch.setattr(
        inputs_cmd,
        "handle_api_error",
        lambda *_args, **_kwargs: None,
    )

    def _fake_post_with_retry(_client, _path, payload):
        captured["payload"] = payload
        return _FakeResponse({"changes": [], "changes_count": 0}, status_code=200)

    monkeypatch.setattr(inputs_cmd, "post_with_retry", _fake_post_with_retry)

    result = runner.invoke(
        inputs_cmd.app,
        [
            "refine",
            "--scenario-id",
            "scenario_1",
            "--input-set-id",
            "set_1",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["payload"]["project_id"] == "project_ctx"
    assert captured["payload"]["scenario_id"] == "scenario_1"
    assert captured["payload"]["input_set_id"] == "set_1"


def test_inputs_synthesize_uses_story_contexts_from_suggested_cache(monkeypatch, tmp_path):
    captured = {}

    monkeypatch.setattr(inputs_cmd.Path, "home", lambda: tmp_path)

    suggested_dir = tmp_path / ".fluxloop" / "personas"
    suggested_dir.mkdir(parents=True, exist_ok=True)
    suggested_path = suggested_dir / "suggested_scenario_1.yaml"
    suggested_path.write_text("persona_ids: []\n")

    monkeypatch.setattr(
        inputs_cmd,
        "load_payload_file",
        lambda _path: {
            "persona_ids": ["p1"],
            "stories": [
                {"id": "story_1", "narrative": "Story one narrative."},
                {"id": "story_2", "narrative": "Story two narrative."},
            ],
            "castings": [
                {
                    "storyId": "story_1",
                    "status": "matched",
                    "personaId": "p1",
                },
                {
                    "storyId": "story_2",
                    "status": "no_match",
                    "reasonCode": "LOW_CONFIDENCE",
                    "bestEffort": {
                        "personaId": "p2",
                    },
                },
            ],
        },
    )
    monkeypatch.setattr(
        inputs_cmd,
        "create_authenticated_client",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        inputs_cmd,
        "spin_while",
        lambda _message, fn, console=None: fn(),
    )
    monkeypatch.setattr(inputs_cmd, "handle_api_error", lambda *_args, **_kwargs: None)

    def _fake_post_with_retry(_client, _path, payload):
        captured["payload"] = payload
        return _FakeResponse(
            {
                "items": [{"input": "example"}],
                "version": 1,
                "persona_ids": payload.get("persona_ids") or [],
            },
            status_code=200,
        )

    monkeypatch.setattr(inputs_cmd, "post_with_retry", _fake_post_with_retry)

    result = runner.invoke(
        inputs_cmd.app,
        [
            "synthesize",
            "--project-id",
            "project_1",
            "--scenario-id",
            "scenario_1",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["payload"]["persona_ids"] == ["p1"]
    assert captured["payload"]["story_contexts"] == [
        {"persona_id": "p1", "story_context": "Story one narrative."}
    ]
