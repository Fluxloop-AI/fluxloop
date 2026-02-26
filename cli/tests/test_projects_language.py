from typer.testing import CliRunner

from fluxloop_cli.commands import projects as projects_cmd

runner = CliRunner()


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload


def _mock_create_stack(monkeypatch, captured_payload):
    """Set up mocks for projects create command."""

    class _FakeClient:
        def get(self, url, **kwargs):
            if "/api/workspaces" in url:
                return _FakeResponse([{"id": "ws_1", "name": "Default"}])
            return _FakeResponse({})

    monkeypatch.setattr(
        projects_cmd,
        "create_authenticated_client",
        lambda *_args, **_kwargs: _FakeClient(),
    )
    monkeypatch.setattr(
        projects_cmd, "handle_api_error", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        projects_cmd, "select_web_project", lambda *_args, **_kwargs: None
    )

    def _fake_post_with_retry(_client, _path, payload):
        captured_payload["payload"] = payload
        return _FakeResponse({"id": "proj_1", "name": payload.get("name", "test")})

    monkeypatch.setattr(projects_cmd, "post_with_retry", _fake_post_with_retry)


def test_create_project_with_language(monkeypatch):
    captured = {}
    _mock_create_stack(monkeypatch, captured)

    result = runner.invoke(
        projects_cmd.app,
        ["create", "--name", "My Project", "--language", "ko"],
    )

    assert result.exit_code == 0, result.output
    assert captured["payload"]["settings"] == {"default_language": "ko"}


def test_create_project_normalizes_language(monkeypatch):
    captured = {}
    _mock_create_stack(monkeypatch, captured)

    result = runner.invoke(
        projects_cmd.app,
        ["create", "--name", "My Project", "--language", "JA-JP"],
    )

    assert result.exit_code == 0, result.output
    assert captured["payload"]["settings"] == {"default_language": "ja"}


def test_create_project_without_language_omits_settings(monkeypatch):
    captured = {}
    _mock_create_stack(monkeypatch, captured)

    result = runner.invoke(
        projects_cmd.app,
        ["create", "--name", "My Project"],
    )

    assert result.exit_code == 0, result.output
    assert "settings" not in captured["payload"]
