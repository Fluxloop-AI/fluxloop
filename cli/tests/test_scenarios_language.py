import json

from typer.testing import CliRunner

from fluxloop_cli.commands import scenarios as scenarios_cmd

runner = CliRunner()


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload


def _mock_create_stack(monkeypatch, captured_payload, project_settings_map=None):
    """Set up mocks for scenarios create command.

    project_settings_map: dict mapping project_id -> settings dict.
    If a plain dict is passed, it's used for all project IDs (backward compat).
    """
    if project_settings_map is None:
        project_settings_map = {}
    if not callable(getattr(project_settings_map, "get", None)):
        # Plain dict passed — wrap so any project_id returns same settings
        _fixed = project_settings_map
        project_settings_map = type("_", (), {"get": lambda self, k, d=None: _fixed})()

    class _FakeClient:
        def get(self, url, **kwargs):
            if "/api/projects/" in url:
                pid = url.rsplit("/", 1)[-1]
                settings = project_settings_map.get(pid, {})
                return _FakeResponse(
                    {"id": pid, "name": "Test", "settings": settings}
                )
            return _FakeResponse({})

    monkeypatch.setattr(
        scenarios_cmd,
        "create_authenticated_client",
        lambda *_args, **_kwargs: _FakeClient(),
    )
    monkeypatch.setattr(
        scenarios_cmd, "handle_api_error", lambda *_args, **_kwargs: None
    )

    def _fake_post_with_retry(_client, _path, payload):
        captured_payload["payload"] = payload
        return _FakeResponse(
            {"scenario_id": "sc_1", "name": payload.get("name", "test")}
        )

    monkeypatch.setattr(scenarios_cmd, "post_with_retry", _fake_post_with_retry)
    monkeypatch.setattr(scenarios_cmd, "set_scenario", lambda *_args, **_kwargs: None)


def test_create_with_explicit_language(monkeypatch):
    captured = {}
    _mock_create_stack(monkeypatch, captured)

    result = runner.invoke(
        scenarios_cmd.app,
        [
            "create",
            "--name", "Test Scenario",
            "--project-id", "proj_1",
            "--language", "ko",
        ],
    )

    assert result.exit_code == 0, result.output
    snapshot = captured["payload"]["config_snapshot"]
    assert snapshot["language"] == "ko"


def test_create_normalizes_language_token(monkeypatch):
    captured = {}
    _mock_create_stack(monkeypatch, captured)

    result = runner.invoke(
        scenarios_cmd.app,
        [
            "create",
            "--name", "Test Scenario",
            "--project-id", "proj_1",
            "--language", "EN-US",
        ],
    )

    assert result.exit_code == 0, result.output
    snapshot = captured["payload"]["config_snapshot"]
    assert snapshot["language"] == "en"


def test_create_falls_back_to_project_default(monkeypatch):
    captured = {}
    _mock_create_stack(
        monkeypatch, captured, project_settings_map={"proj_1": {"default_language": "ja"}}
    )

    result = runner.invoke(
        scenarios_cmd.app,
        [
            "create",
            "--name", "Test Scenario",
            "--project-id", "proj_1",
        ],
    )

    assert result.exit_code == 0, result.output
    snapshot = captured["payload"]["config_snapshot"]
    assert snapshot["language"] == "ja"


def test_create_falls_back_to_en_when_no_project_language(monkeypatch):
    captured = {}
    _mock_create_stack(monkeypatch, captured, project_settings_map={"proj_1": {}})

    result = runner.invoke(
        scenarios_cmd.app,
        [
            "create",
            "--name", "Test Scenario",
            "--project-id", "proj_1",
        ],
    )

    assert result.exit_code == 0, result.output
    snapshot = captured["payload"]["config_snapshot"]
    assert snapshot["language"] == "en"


def test_create_explicit_language_overrides_project_default(monkeypatch):
    captured = {}
    _mock_create_stack(
        monkeypatch, captured, project_settings_map={"proj_1": {"default_language": "ja"}}
    )

    result = runner.invoke(
        scenarios_cmd.app,
        [
            "create",
            "--name", "Test Scenario",
            "--project-id", "proj_1",
            "--language", "ko",
        ],
    )

    assert result.exit_code == 0, result.output
    snapshot = captured["payload"]["config_snapshot"]
    assert snapshot["language"] == "ko"


def test_file_without_language_gets_fallback(monkeypatch, tmp_path):
    """--file that has config_snapshot but no language should still get fallback."""
    captured = {}
    _mock_create_stack(
        monkeypatch, captured, project_settings_map={"proj_2": {"default_language": "fr"}}
    )

    payload_file = tmp_path / "payload.json"
    payload_file.write_text(
        json.dumps(
            {
                "name": "From File",
                "project_id": "proj_2",
                "config_snapshot": {"goal": "test goal"},
            }
        )
    )

    result = runner.invoke(
        scenarios_cmd.app,
        [
            "create",
            "--name", "Ignored",
            "--project-id", "proj_1",
            "--file", str(payload_file),
        ],
    )

    assert result.exit_code == 0, result.output
    payload = captured["payload"]
    # project_id should come from --file, not --project-id
    assert payload["project_id"] == "proj_2"
    assert payload["config_snapshot"]["language"] == "fr"


def test_file_with_language_preserved(monkeypatch, tmp_path):
    """--file that already has language should keep it (normalized)."""
    captured = {}
    _mock_create_stack(monkeypatch, captured)

    payload_file = tmp_path / "payload.json"
    payload_file.write_text(
        json.dumps(
            {
                "name": "From File",
                "project_id": "proj_1",
                "config_snapshot": {"goal": "test", "language": "ZH-TW"},
            }
        )
    )

    result = runner.invoke(
        scenarios_cmd.app,
        [
            "create",
            "--name", "Ignored",
            "--project-id", "proj_1",
            "--file", str(payload_file),
        ],
    )

    assert result.exit_code == 0, result.output
    # language from file should be normalized
    assert captured["payload"]["config_snapshot"]["language"] == "zh"


def test_file_language_beats_explicit_flag(monkeypatch, tmp_path):
    """Language in --file takes priority over --language flag."""
    captured = {}
    _mock_create_stack(monkeypatch, captured)

    payload_file = tmp_path / "payload.json"
    payload_file.write_text(
        json.dumps(
            {
                "name": "From File",
                "project_id": "proj_1",
                "config_snapshot": {"goal": "test", "language": "ja"},
            }
        )
    )

    result = runner.invoke(
        scenarios_cmd.app,
        [
            "create",
            "--name", "Ignored",
            "--project-id", "proj_1",
            "--language", "ko",
            "--file", str(payload_file),
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["payload"]["config_snapshot"]["language"] == "ja"
