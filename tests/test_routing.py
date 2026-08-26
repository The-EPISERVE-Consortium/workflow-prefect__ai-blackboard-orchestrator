from routing import ROUTES


def test_bug_report_route_includes_row_id_and_result():
    row = {"id": 42, "task_type": "bug-report", "result": "finding: X is broken"}

    prompt = ROUTES["bug-report"](row)

    assert "task_runs.id=42" in prompt
    assert "finding: X is broken" in prompt


def test_unknown_task_type_has_no_route():
    assert "does-not-exist" not in ROUTES
