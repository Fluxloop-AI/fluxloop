from typer.testing import CliRunner

from fluxloop_cli.commands import personas as personas_cmd


runner = CliRunner()


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def _mock_suggest_stack(monkeypatch, captured_payload, captured_cache=None):
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
    def _fake_save_cache_file(_subdir, _filename, data):
        if isinstance(captured_cache, dict):
            captured_cache["data"] = data
        return "/tmp/suggested.yaml"

    monkeypatch.setattr(personas_cmd, "save_cache_file", _fake_save_cache_file)

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
                "stories": [
                    {
                        "id": "story_1",
                        "narrative": "A narrative.",
                    }
                ],
                "castings": [
                    {
                        "storyId": "story_1",
                        "status": "no_match",
                        "reasonCode": "LOW_CONFIDENCE",
                        "message": "No high-confidence persona found.",
                        "bestEffort": {
                            "personaId": "p1",
                            "personaName": "Persona 1",
                            "source": "admin_template",
                            "score": 0.45,
                        },
                    }
                ],
                "strategy": {
                    "coverageNote": "Coverage gap exists.",
                    "diversityNote": "Diversity constraints not applied.",
                    "fallbackNote": "Use human-in-the-loop review.",
                },
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


def test_personas_suggest_caches_stories_and_castings(monkeypatch):
    captured = {}
    cached = {}
    _mock_suggest_stack(monkeypatch, captured, cached)

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
    assert cached["data"]["persona_ids"] == ["p1"]
    assert cached["data"]["stories"][0]["id"] == "story_1"
    assert cached["data"]["castings"][0]["storyId"] == "story_1"


def test_personas_suggest_accepts_inline_stories(monkeypatch):
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
            "--stories",
            '[{"id":"story_inline_1","title":"Inline story"}]',
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["payload"]["stories"][0]["id"] == "story_inline_1"


def test_personas_suggest_inline_stories_supports_shorthand(monkeypatch):
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
            "--stories",
            '["First shorthand story", {"title":"Second shorthand story"}]',
        ],
    )

    assert result.exit_code == 0, result.output
    stories = captured["payload"]["stories"]
    assert len(stories) == 2
    assert stories[0]["id"] == "story_1"
    assert stories[0]["title"] == "First shorthand story"
    assert stories[0]["narrative"]
    assert stories[0]["testFocus"]
    assert stories[0]["castingQuery"]
    assert stories[0]["protagonistProfile"]["description"]
    assert stories[0]["protagonistProfile"]["idealType"]
    assert stories[1]["id"] == "story_2"
    assert stories[1]["title"] == "Second shorthand story"


def test_personas_suggest_accepts_stories_file(monkeypatch, tmp_path):
    captured = {}
    _mock_suggest_stack(monkeypatch, captured)

    stories_path = tmp_path / "stories.json"
    stories_path.write_text('{"stories":[{"id":"story_file_1","title":"File story"}]}')

    result = runner.invoke(
        personas_cmd.app,
        [
            "suggest",
            "--project-id",
            "project_1",
            "--scenario-id",
            "scenario_1",
            "--stories-file",
            str(stories_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["payload"]["stories"][0]["id"] == "story_file_1"


def test_personas_suggest_prints_story_casting_details(monkeypatch):
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
    assert "Story Casting" in result.output
    assert "no_match" in result.output
    assert "LOW_CONFIDENCE: No high-confidence persona found." in result.output
    assert "Best-effort: Persona 1 (0.4500)" in result.output
    assert "Coverage: Coverage gap exists." in result.output
    assert "Diversity: Diversity constraints not applied." in result.output
    assert "Fallback: Use human-in-the-loop review." in result.output
