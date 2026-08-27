from routing import ROUTES, _extract_section

_FULL_REPORT = """---
title: Code Analysis Report
subtitle: org/repo
---

Some intro prose that shouldn't end up in the fix prompt.

## Executive Summary

This section should be excluded from the fix prompt.

## Potential Bug Analysis

| Priority | Potential Bug |
|---|---|
| Medium | Something is broken here |

## Vulnerability Analysis

This section should also be excluded.
"""


def test_code_analysis_report_route_includes_row_id_and_result():
    row = {"id": 42, "task_type": "code-analysis-report", "finding": "finding: X is broken", "prompt": None}

    prompt = ROUTES["code-analysis-report"](row)

    assert "task_runs.id=42" in prompt
    assert "finding: X is broken" in prompt


def test_code_analysis_report_route_extracts_only_bug_analysis_section():
    row = {"id": 46, "task_type": "code-analysis-report", "finding": _FULL_REPORT, "prompt": None}

    prompt = ROUTES["code-analysis-report"](row)

    assert "Something is broken here" in prompt
    assert "Executive Summary" not in prompt
    assert "shouldn't end up in the fix prompt" not in prompt
    assert "Vulnerability Analysis" not in prompt
    assert "should also be excluded" not in prompt


def test_code_analysis_report_route_falls_back_to_full_finding_without_bug_analysis_heading():
    row = {"id": 47, "task_type": "code-analysis-report", "finding": "no headings here at all", "prompt": None}

    prompt = ROUTES["code-analysis-report"](row)

    assert "no headings here at all" in prompt


def test_extract_section_returns_none_when_heading_missing():
    assert _extract_section("no headings here", "Potential Bug Analysis") is None


def test_extract_section_stops_at_next_level_two_heading():
    text = "## Potential Bug Analysis\n\nbody text\n\n## Next Section\n\nother text\n"

    assert _extract_section(text, "Potential Bug Analysis") == "body text"


def test_extract_section_reads_to_end_of_document_when_last_section():
    text = "## Other\n\nx\n\n## Potential Bug Analysis\n\nlast section body\n"

    assert _extract_section(text, "Potential Bug Analysis") == "last section body"


def test_code_analysis_report_route_asks_for_a_fix_summary_publish():
    row = {"id": 45, "task_type": "code-analysis-report", "finding": "finding: W is broken", "prompt": None}

    prompt = ROUTES["code-analysis-report"](row)

    assert "task_type='fix-summary'" in prompt


def test_code_analysis_report_route_includes_source_prompt_when_present():
    row = {
        "id": 43, "task_type": "code-analysis-report", "finding": "finding: Y is broken",
        "prompt": "Clone https://github.com/org/repo, analyse it and write a report.",
    }

    prompt = ROUTES["code-analysis-report"](row)

    assert "Clone https://github.com/org/repo" in prompt


def test_code_analysis_report_route_falls_back_when_prompt_missing():
    row = {"id": 44, "task_type": "code-analysis-report", "finding": "finding: Z is broken", "prompt": None}

    prompt = ROUTES["code-analysis-report"](row)

    assert "wasn't recorded" in prompt


def test_unknown_task_type_has_no_route():
    assert "does-not-exist" not in ROUTES
