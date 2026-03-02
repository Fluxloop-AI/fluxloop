import json
from pathlib import Path

import typer
from typer.testing import CliRunner

from fluxloop_cli.commands import data as data_cmd


runner = CliRunner()


class _FakeResponse:
    def __init__(self, payload, status_code: int = 200, text: str | None = None):
        self._payload = payload
        self.status_code = status_code
        self.is_success = 200 <= status_code < 300
        self.text = text if text is not None else json.dumps(payload, ensure_ascii=False, default=str)

    def json(self):
        return self._payload


def _fake_handle_api_error(resp, _context):
    if not resp.is_success:
        raise typer.Exit(1)


def _setup_push_stack(
    monkeypatch,
    captured: dict,
    *,
    materialize_payload: dict | None = None,
) -> None:
    class _FakeClient:
        def post(self, path, json=None, **_kwargs):
            if path == "/api/projects/proj_1/data":
                captured["create_payload"] = json
                return _FakeResponse(
                    {
                        "data": {"id": "data_1"},
                        "upload": {
                            "upload_url": "https://upload.example.com/data_1",
                            "headers": {"x-test": "1"},
                        },
                    }
                )
            if path == "/api/projects/proj_1/data/data_1/confirm":
                captured["confirm_payload"] = json
                return _FakeResponse({"processing_status": "completed"})
            if path == "/api/scenarios/sc_1/data/bind":
                captured["bind_payload"] = json
                return _FakeResponse({"ok": True})
            if path == "/api/scenarios/sc_1/ground-truth/materialize":
                captured["materialize_payload"] = json
                return _FakeResponse(
                    materialize_payload
                    or {
                        "profile": {"id": "gt_profile_1"},
                        "gt_contracts": [{"id": "gtc_1"}, {"id": "gtc_2"}],
                        "binding": {"binding_meta": {"role": "validation"}},
                    }
                )
            raise AssertionError(f"unexpected POST path: {path}")

    monkeypatch.setattr(data_cmd, "create_authenticated_client", lambda *_args, **_kwargs: _FakeClient())
    monkeypatch.setattr(data_cmd, "handle_api_error", _fake_handle_api_error)
    monkeypatch.setattr(data_cmd.httpx, "put", lambda *_args, **_kwargs: _FakeResponse({}, status_code=200))
    monkeypatch.setattr(data_cmd, "get_current_web_project_id", lambda: "proj_1")


def _setup_bind_stack(
    monkeypatch,
    captured: dict,
    *,
    bind_status_code: int = 200,
    materialize_payload: dict | None = None,
    materialize_status_code: int = 200,
    materialize_text: str | None = None,
) -> None:
    class _FakeClient:
        def post(self, path, json=None, **_kwargs):
            if path == "/api/scenarios/sc_1/data/bind":
                captured["bind_payload"] = json
                return _FakeResponse({"ok": True}, status_code=bind_status_code)
            if path == "/api/scenarios/sc_1/ground-truth/materialize":
                captured["materialize_payload"] = json
                return _FakeResponse(
                    materialize_payload
                    or {
                        "profile": {"id": "gt_profile_1"},
                        "gt_contracts": [{"id": "gtc_1"}, {"id": "gtc_2"}],
                    },
                    status_code=materialize_status_code,
                    text=materialize_text,
                )
            raise AssertionError(f"unexpected POST path: {path}")

    monkeypatch.setattr(data_cmd, "create_authenticated_client", lambda *_args, **_kwargs: _FakeClient())
    monkeypatch.setattr(data_cmd, "handle_api_error", _fake_handle_api_error)


def test_push_ground_truth_requires_bind_or_scenario(tmp_path: Path):
    file_path = tmp_path / "dataset.csv"
    file_path.write_text("q,a\nhello,world\n")

    result = runner.invoke(
        data_cmd.app,
        [
            "push",
            str(file_path),
            "--usage",
            "ground-truth",
        ],
    )

    assert result.exit_code != 0
    assert "--usage ground-truth requires --bind or --scenario" in result.output


def test_bind_gt_options_require_validation_role(monkeypatch):
    monkeypatch.setattr(data_cmd, "get_current_scenario_id", lambda: "sc_1")

    result = runner.invoke(
        data_cmd.app,
        [
            "bind",
            "data_1",
            "--role",
            "input",
            "--split",
            "test",
        ],
    )

    assert result.exit_code != 0
    assert "--role validation" in result.output


def test_bind_sampling_seed_42_explicit_requires_validation_role(monkeypatch):
    monkeypatch.setattr(data_cmd, "get_current_scenario_id", lambda: "sc_1")

    result = runner.invoke(
        data_cmd.app,
        [
            "bind",
            "data_1",
            "--role",
            "input",
            "--sampling-seed",
            "42",
        ],
    )

    assert result.exit_code != 0
    assert "--role validation" in result.output


def test_push_ground_truth_forces_dataset_payload(monkeypatch, tmp_path: Path):
    captured: dict = {}
    _setup_push_stack(monkeypatch, captured)

    file_path = tmp_path / "qa.csv"
    file_path.write_text("question,answer\nQ1,A1\n")

    result = runner.invoke(
        data_cmd.app,
        [
            "push",
            str(file_path),
            "--usage",
            "ground-truth",
            "--scenario",
            "sc_1",
            "--as",
            "document",
            "--split",
            "test",
            "--label-column",
            "answer",
            "--row-filter",
            "lang = 'ko'",
            "--sampling-seed",
            "7",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["create_payload"]["data_category"] == "DATASET"
    assert captured["create_payload"]["processing_profile"] == "dataset"
    assert captured["create_payload"]["file_type"] == "structured"
    assert captured["bind_payload"]["binding_meta"]["role"] == "validation"
    assert captured["materialize_payload"]["data_id"] == "data_1"
    assert captured["materialize_payload"]["split"] == "test"
    assert captured["materialize_payload"]["label_column"] == "answer"
    assert captured["materialize_payload"]["row_filter"] == "lang = 'ko'"
    assert captured["materialize_payload"]["sampling_seed"] == 7


def test_push_context_sampling_seed_42_explicit_rejected(tmp_path: Path):
    file_path = tmp_path / "requirements.md"
    file_path.write_text("# Requirements\n")

    result = runner.invoke(
        data_cmd.app,
        [
            "push",
            str(file_path),
            "--usage",
            "context",
            "--sampling-seed",
            "42",
        ],
    )

    assert result.exit_code != 0
    assert "valid with --usage ground-truth" in result.output


def test_push_ground_truth_bind_without_context_scenario_fails(monkeypatch, tmp_path: Path):
    file_path = tmp_path / "dataset.csv"
    file_path.write_text("q,a\nhello,world\n")
    monkeypatch.setattr(data_cmd, "get_current_scenario_id", lambda: None)

    result = runner.invoke(
        data_cmd.app,
        [
            "push",
            str(file_path),
            "--usage",
            "ground-truth",
            "--bind",
        ],
    )

    assert result.exit_code == 1
    assert "Ground Truth upload requires a scenario binding." in result.output


def test_bind_validation_sends_gt_binding_meta(monkeypatch):
    captured: dict = {}
    _setup_bind_stack(monkeypatch, captured)

    result = runner.invoke(
        data_cmd.app,
        [
            "bind",
            "data_1",
            "--scenario",
            "sc_1",
            "--role",
            "validation",
            "--split",
            "dev",
            "--label-column",
            "expected",
            "--row-filter",
            "lang = 'ko'",
            "--sampling-seed",
            "99",
            "--no-materialize-gt",
        ],
    )

    assert result.exit_code == 0, result.output
    binding_meta = captured["bind_payload"]["binding_meta"]
    assert binding_meta["role"] == "validation"
    assert binding_meta["split"] == "dev"
    assert binding_meta["label_column"] == "expected"
    assert binding_meta["row_filter"] == "lang = 'ko'"
    assert binding_meta["sampling_seed"] == 99
    assert "materialize_payload" not in captured


def test_bind_validation_materialize_outputs_profile_and_contract_count(monkeypatch):
    captured: dict = {}
    _setup_bind_stack(
        monkeypatch,
        captured,
        materialize_payload={
            "profile": {"id": "gt_profile_99"},
            "gt_contracts": [{"id": "gtc_1"}, {"id": "gtc_2"}, {"id": "gtc_3"}],
        },
    )

    result = runner.invoke(
        data_cmd.app,
        [
            "bind",
            "data_1",
            "--scenario",
            "sc_1",
            "--role",
            "validation",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "profile_id: gt_profile_99" in result.output
    assert "gt_contract_count: 3" in result.output
    assert captured["materialize_payload"]["data_id"] == "data_1"


def test_bind_validation_materialize_409_processing_actionable(monkeypatch):
    captured: dict = {}
    _setup_bind_stack(
        monkeypatch,
        captured,
        materialize_status_code=409,
        materialize_payload={"detail": "Dataset processing is pending."},
    )

    result = runner.invoke(
        data_cmd.app,
        [
            "bind",
            "data_1",
            "--scenario",
            "sc_1",
            "--role",
            "validation",
        ],
    )

    assert result.exit_code == 1
    assert "Ground Truth materialization failed (409)" in result.output
    assert "Wait for dataset processing" in result.output
    assert "fluxloop data show data_1" in result.output


def test_bind_validation_materialize_409_role_actionable(monkeypatch):
    captured: dict = {}
    _setup_bind_stack(
        monkeypatch,
        captured,
        materialize_status_code=409,
        materialize_payload={"detail": "role is not validation for this binding"},
    )

    result = runner.invoke(
        data_cmd.app,
        [
            "bind",
            "data_1",
            "--scenario",
            "sc_1",
            "--role",
            "validation",
        ],
    )

    assert result.exit_code == 1
    assert "Ground Truth materialization failed (409)" in result.output
    assert "Ensure validation role binding" in result.output
    assert "fluxloop data gt status --scenario sc_1 --data-id" in result.output
    assert "data_1" in result.output


def test_bind_validation_materialize_generic_error_actionable(monkeypatch):
    captured: dict = {}
    _setup_bind_stack(
        monkeypatch,
        captured,
        materialize_status_code=500,
        materialize_payload={"detail": "internal error"},
    )

    result = runner.invoke(
        data_cmd.app,
        [
            "bind",
            "data_1",
            "--scenario",
            "sc_1",
            "--role",
            "validation",
        ],
    )

    assert result.exit_code == 1
    assert "Ground Truth materialization failed (500)" in result.output
    assert "Inspect processing state: fluxloop data show data_1" in result.output


def test_bind_quiet_outputs_core_identifiers(monkeypatch):
    captured: dict = {}
    _setup_bind_stack(
        monkeypatch,
        captured,
        materialize_payload={
            "profile": {"id": "gt_profile_q"},
            "gt_contracts": [{"id": "gtc_1"}],
        },
    )

    result = runner.invoke(
        data_cmd.app,
        [
            "bind",
            "data_1",
            "--scenario",
            "sc_1",
            "--role",
            "validation",
            "--quiet",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "data_id=data_1" in result.output
    assert "scenario_id=sc_1" in result.output
    assert "profile_id=gt_profile_q" in result.output
    assert "gt_contract_count=1" in result.output
    assert "Validation (GT) binding complete" not in result.output


def test_gt_status_table_output(monkeypatch):
    class _FakeClient:
        def get(self, path, params=None, **_kwargs):
            assert path == "/api/scenarios/sc_1/ground-truth/status"
            assert params == {"data_id": "data_1"}
            return _FakeResponse(
                {
                    "items": [
                        {
                            "data_id": "data_1",
                            "materialization_status": "ready",
                            "ground_truth_profile_id": "gt_profile_1",
                            "gt_contract_ids": ["gtc_1", "gtc_2"],
                            "processing_status": "completed",
                            "updated_at": "2026-03-02T12:00:00Z",
                        }
                    ]
                }
            )

    monkeypatch.setattr(data_cmd, "create_authenticated_client", lambda *_args, **_kwargs: _FakeClient())
    monkeypatch.setattr(data_cmd, "handle_api_error", _fake_handle_api_error)

    result = runner.invoke(
        data_cmd.app,
        [
            "gt",
            "status",
            "--scenario",
            "sc_1",
            "--data-id",
            "data_1",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Ground Truth Status" in result.output
    assert "data_id" in result.output
    assert "data_1" in result.output
    assert "ready" in result.output
    assert "gt_profile" in result.output
    assert "2" in result.output


def test_gt_status_json_output(monkeypatch):
    class _FakeClient:
        def get(self, path, params=None, **_kwargs):
            assert path == "/api/scenarios/sc_1/ground-truth/status"
            assert params is None
            return _FakeResponse(
                {
                    "items": [
                        {
                            "data_id": "data_1",
                            "materialization_status": "ready",
                            "ground_truth_profile_id": "gt_profile_1",
                            "gt_contract_ids": ["gtc_1", "gtc_2"],
                            "processing_status": "completed",
                            "updated_at": "2026-03-02T12:00:00Z",
                        }
                    ]
                }
            )

    monkeypatch.setattr(data_cmd, "create_authenticated_client", lambda *_args, **_kwargs: _FakeClient())
    monkeypatch.setattr(data_cmd, "handle_api_error", _fake_handle_api_error)

    result = runner.invoke(
        data_cmd.app,
        [
            "gt",
            "status",
            "--scenario",
            "sc_1",
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert '"data_id"' in result.output
    assert '"data_1"' in result.output
    assert '"gt_contract_count"' in result.output


def test_build_gt_status_rows_handles_varied_payload_shapes():
    rows_from_items = data_cmd._build_gt_status_rows(
        {
            "items": [
                {
                    "data_id": "data_items",
                    "materialization_status": "ready",
                    "ground_truth_profile_id": "profile_items",
                    "gt_contract_ids": ["a", "b"],
                    "processing_status": "completed",
                }
            ]
        }
    )
    assert len(rows_from_items) == 1
    assert rows_from_items[0]["data_id"] == "data_items"
    assert rows_from_items[0]["gt_contract_count"] == 2

    rows_from_statuses = data_cmd._build_gt_status_rows(
        {
            "statuses": [
                {
                    "profile": {"id": "profile_nested"},
                    "gt_contracts": [{"id": "c1"}],
                }
            ]
        },
        fallback_data_id="fallback_data",
    )
    assert len(rows_from_statuses) == 1
    assert rows_from_statuses[0]["data_id"] == "fallback_data"
    assert rows_from_statuses[0]["ground_truth_profile_id"] == "profile_nested"
    assert rows_from_statuses[0]["gt_contract_count"] == 1

    rows_from_single = data_cmd._build_gt_status_rows(
        {"materialization_status": "pending"},
        fallback_data_id="single_data",
    )
    assert len(rows_from_single) == 1
    assert rows_from_single[0]["data_id"] == "single_data"
    assert rows_from_single[0]["materialization_status"] == "pending"

    rows_from_invalid = data_cmd._build_gt_status_rows("invalid")
    assert rows_from_invalid == []


def test_push_context_regression_keeps_legacy_payload(monkeypatch, tmp_path: Path):
    captured: dict = {}
    _setup_push_stack(monkeypatch, captured)

    file_path = tmp_path / "requirements.md"
    file_path.write_text("# Requirements\n")

    result = runner.invoke(
        data_cmd.app,
        [
            "push",
            str(file_path),
            "--project-id",
            "proj_1",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["create_payload"]["data_category"] == "KNOWLEDGE"
    assert captured["create_payload"]["file_type"] == "document"
    assert captured["create_payload"]["processing_profile"] == "auto"


def test_bind_regression_non_validation_role(monkeypatch):
    captured: dict = {}
    _setup_bind_stack(monkeypatch, captured)

    result = runner.invoke(
        data_cmd.app,
        [
            "bind",
            "data_1",
            "--scenario",
            "sc_1",
            "--role",
            "input",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["bind_payload"]["binding_meta"] == {"role": "input"}
    assert "materialize_payload" not in captured
