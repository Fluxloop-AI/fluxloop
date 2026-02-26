from typer.testing import CliRunner

from fluxloop_cli.commands import personas as personas_cmd


runner = CliRunner()


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def _mock_suggest_stack(monkeypatch, captured_payload):
    monkeypatch.setattr(
        personas_cmd,
        "create_authenticated_client",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        personas_cmd,
        "spin_while",
        lambda _message, fn, console=None: fn(),
    )
    monkeypatch.setattr(personas_cmd, "handle_api_error", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        personas_cmd,
        "save_cache_file",
        lambda *_args, **_kwargs: "/tmp/suggested.yaml",
    )

    def _fake_post_with_retry(_client, _path, payload):
        captured_payload["payload"] = payload
        return _FakeResponse(
            {
                "persona_ids": ["p1"],
                "personas": [
                    {
                        "id": "p1",
                        "name": "Persona 1",
                        "attributes": {
                            "difficulty": "medium",
                            "character_summary": "sample",
                        },
                    }
                ],
            }
        )

    monkeypatch.setattr(personas_cmd, "post_with_retry", _fake_post_with_retry)


def test_personas_suggest_includes_normalized_language(monkeypatch):
    captured = {}
    _mock_suggest_stack(monkeypatch, captured)

    result = runner.invoke(
        personas_cmd.app,
        [
            "suggest",
            "--project-id",
            "project_1",
            "--scenario-id",
            "scenario_1",
            "--language",
            "EN-US",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["payload"]["project_id"] == "project_1"
    assert captured["payload"]["scenario_id"] == "scenario_1"
    assert captured["payload"]["language"] == "en"


def test_personas_suggest_omits_language_when_not_given(monkeypatch):
    captured = {}
    _mock_suggest_stack(monkeypatch, captured)

    result = runner.invoke(
        personas_cmd.app,
        [
            "suggest",
            "--project-id",
            "project_1",
            "--scenario-id",
            "scenario_1",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "language" not in captured["payload"]
