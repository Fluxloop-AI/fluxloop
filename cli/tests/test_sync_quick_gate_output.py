from fluxloop_cli.commands import sync as sync_cmd


def test_quick_gate_summary_shows_preliminary_and_note(capsys):
    quick_gate = {
        "disclaimer": "informational_only",
        "total_runs": 20,
        "gate_applicable_runs": 15,
        "gate_pass_runs": 12,
        "gate_fail_runs": 3,
        "gate_pass_rate": 0.8,
    }

    sync_cmd._print_quick_gate_summary(quick_gate)

    output = capsys.readouterr().out
    assert "GT Gate (preliminary): 12/15 pass (80.0%)" in output
    assert "preliminary result (informational_only)" in output


def test_quick_gate_summary_shows_na_when_no_applicable_runs(capsys):
    quick_gate = {
        "disclaimer": "informational_only",
        "total_runs": 10,
        "gate_applicable_runs": 0,
        "gate_pass_runs": 0,
        "gate_fail_runs": 0,
        "gate_pass_rate": None,
    }

    sync_cmd._print_quick_gate_summary(quick_gate)

    output = capsys.readouterr().out
    assert "GT Gate (preliminary): 0/0 pass (N/A)" in output
