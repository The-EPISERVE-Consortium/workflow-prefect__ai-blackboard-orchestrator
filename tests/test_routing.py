from routing import ROUTES


def test_code_analysis_report_route_includes_row_id_and_result():
    row = {"id": 42, "task_type": "code-analysis-report", "result": "finding: X is broken", "prompt": None}

    prompt = ROUTES["code-analysis-report"](row)

    assert "task_runs.id=42" in prompt
    assert "finding: X is broken" in prompt


def test_code_analysis_report_route_includes_source_prompt_when_present():
    row = {
        "id": 43, "task_type": "code-analysis-report", "result": "finding: Y is broken",
        "prompt": "Clone https://github.com/org/repo, analyse it and write a report.",
    }

    prompt = ROUTES["code-analysis-report"](row)

    assert "Clone https://github.com/org/repo" in prompt


def test_code_analysis_report_route_falls_back_when_prompt_missing():
    row = {"id": 44, "task_type": "code-analysis-report", "result": "finding: Z is broken", "prompt": None}

    prompt = ROUTES["code-analysis-report"](row)

    assert "wasn't recorded" in prompt


def test_unknown_task_type_has_no_route():
    assert "does-not-exist" not in ROUTES
