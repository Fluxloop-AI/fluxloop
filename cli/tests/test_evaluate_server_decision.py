import json

import typer
from typer.testing import CliRunner

from fluxloop_cli.commands import evaluate as evaluate_cmd


runner = CliRunner()


class _FakeResponse:
    def __init__(self, payload, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self.is_success = 200 <= status_code < 300
        self.text = json.dumps(payload, ensure_ascii=False, default=str)

    def json(self):
        return self._payload


def _fake_handle_api_error(resp, _context):
    if not resp.is_success:
        raise typer.Exit(1)


def test_show_decision_renders_text_and_calls_decision_api(monkeypatch):
    trigger_payload = {"evaluation_id": "eval_1", "status": "queued"}
    decision_payload = {
        "evaluation_id": "eval_1",
        "experiment_id": "exp_1",
        "release_decision": "review",
        "decision_snapshot": {
            "overall_verdict": "fail",
            "metrics": {"tokens_used": 12450, "cost_usd": 0.38},
            "gate_results": [
                {"gate_key": "run:warning_count", "status": "warn"},
            ],
        },
        "gate_results_snapshot": [
            {"gate_key": "run:fail_count", "status": "pass"},
            {
                "gate_key": "ground_truth:deterministic",
                "status": "fail",
                "reason": "coverage_below_threshold:2; violated_constraints:1",
            },
        ],
    }

    class _FakeClient:
        def __init__(self):
            self.get_calls = []

        def get(self, path, params=None):
            self.get_calls.append((path, params))
            if path == "/api/experiments/exp_1/decision":
                return _FakeResponse(decision_payload)
            raise AssertionError(f"unexpected GET path: {path}")

    fake_client = _FakeClient()
    monkeypatch.setattr(
        evaluate_cmd, "create_authenticated_client", lambda *_args, **_kwargs: fake_client
    )
    monkeypatch.setattr(
        evaluate_cmd, "post_with_retry", lambda *_args, **_kwargs: _FakeResponse(trigger_payload)
    )
    monkeypatch.setattr(evaluate_cmd, "handle_api_error", _fake_handle_api_error)

    result = runner.invoke(
        evaluate_cmd.app,
        [
            "--experiment-id",
            "exp_1",
            "--project-id",
            "proj_1",
            "--show-decision",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Release Decision: review" in result.output
    assert "Overall Verdict: fail" in result.output
    assert "run:fail_count => pass" in result.output
    normalized_output = " ".join(result.output.split())
    assert (
        "ground_truth:deterministic => fail (coverage_below_threshold, violated_constraints)"
        in normalized_output
    )
    assert "tokens_used: 12450" in result.output
    assert "cost_usd: 0.38" in result.output
    assert fake_client.get_calls == [
        ("/api/experiments/exp_1/decision", {"project_id": "proj_1"})
    ]


def test_show_decision_json_prints_raw_payload(monkeypatch):
    trigger_payload = {"evaluation_id": "eval_2", "status": "queued"}
    decision_payload = {
        "evaluation_id": "eval_2",
        "experiment_id": "exp_2",
        "release_decision": "ready",
        "decision_snapshot": {"overall_verdict": "pass"},
        "gate_results_snapshot": [],
    }

    class _FakeClient:
        def get(self, path, params=None):
            assert path == "/api/experiments/exp_2/decision"
            assert params == {"project_id": "proj_2"}
            return _FakeResponse(decision_payload)

    monkeypatch.setattr(
        evaluate_cmd, "create_authenticated_client", lambda *_args, **_kwargs: _FakeClient()
    )
    monkeypatch.setattr(
        evaluate_cmd, "post_with_retry", lambda *_args, **_kwargs: _FakeResponse(trigger_payload)
    )
    monkeypatch.setattr(evaluate_cmd, "handle_api_error", _fake_handle_api_error)

    result = runner.invoke(
        evaluate_cmd.app,
        [
            "--experiment-id",
            "exp_2",
            "--project-id",
            "proj_2",
            "--show-decision",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert '"release_decision": "ready"' in result.output
    assert '"overall_verdict": "pass"' in result.output
    assert "Gates:" not in result.output


def test_show_decision_empty_payload_exits_with_clear_message(monkeypatch):
    trigger_payload = {"evaluation_id": "eval_3", "status": "queued"}
    empty_decision_payload = {
        "evaluation_id": None,
        "experiment_id": "exp_3",
        "release_decision": None,
        "decision_snapshot": None,
        "gate_snapshot": None,
        "gate_results_snapshot": None,
    }

    class _FakeClient:
        def get(self, path, params=None):
            assert path == "/api/experiments/exp_3/decision"
            assert params == {"project_id": "proj_3"}
            return _FakeResponse(empty_decision_payload)

    monkeypatch.setattr(
        evaluate_cmd, "create_authenticated_client", lambda *_args, **_kwargs: _FakeClient()
    )
    monkeypatch.setattr(
        evaluate_cmd, "post_with_retry", lambda *_args, **_kwargs: _FakeResponse(trigger_payload)
    )
    monkeypatch.setattr(evaluate_cmd, "handle_api_error", _fake_handle_api_error)

    result = runner.invoke(
        evaluate_cmd.app,
        [
            "--experiment-id",
            "exp_3",
            "--project-id",
            "proj_3",
            "--show-decision",
        ],
    )

    assert result.exit_code == 1
    assert "Decision is not available yet for this experiment." in result.output


def test_render_decision_text_falls_back_to_decision_snapshot_gate_results():
    payload = {
        "release_decision": "review",
        "decision_snapshot": {
            "overall_verdict": "fail",
            "gate_results": [
                {"gate_key": "run:warning_count", "status": "warn", "reason": "limit_exceeded:3"},
            ],
            "metrics": {},
        },
        "gate_results_snapshot": None,
    }

    rendered = evaluate_cmd._render_decision_text(payload)
    assert "run:warning_count => warn (limit_exceeded)" in rendered
