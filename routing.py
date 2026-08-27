"""task_type -> prompt-builder registry, for `post_type='someone_take_over'`
blackboard rows.

Each entry maps a `task_runs.task_type` value to a function that turns that
row's `finding` into the prompt for a follow-up one-shot agent run. Add an
entry here to teach the orchestrator about a new kind of chainable result --
no other code needs to change.

Only `post_type='someone_take_over'` rows are routed through this registry --
a `post_type='run_me'` row already carries its own literal `prompt` and
bypasses this file entirely (see `flow/orchestrator_flow.py`'s
`build_prompt`). A `someone_take_over` row whose `task_type` has no entry
here is left eligible rather than guessed at.
"""

import re
from typing import Callable

PromptBuilder = Callable[[dict], str]


def _extract_section(markdown: str, heading: str) -> str | None:
    """Return the body of one `## heading` section from a markdown report.

    Args:
        markdown: The full report text.
        heading: The level-2 heading to extract, without the `##` prefix
            (e.g. "Potential Bug Analysis").

    Returns:
        Everything between that heading and the next level-2 heading (or the
        end of the document), stripped. None if the heading isn't present --
        callers should fall back to something reasonable rather than assume
        every report has this exact structure.
    """
    match = re.search(
        rf"^##\s+{re.escape(heading)}\s*$\n(.*?)(?=^##\s+|\Z)",
        markdown,
        re.MULTILINE | re.DOTALL,
    )
    return match.group(1).strip() if match else None


def _code_analysis_report_to_fix_prompt(row: dict) -> str:
    # `prompt` is the literal task that produced this report (and almost
    # always contains the actual clonable repo URL) -- rows published before
    # blackboard-communication started capturing it will have it as NULL,
    # so fall back to hoping the URL survived into the report text itself.
    source_prompt = row.get("prompt")
    origin = (
        f"That report was produced by this task: {source_prompt}\n\n"
        if source_prompt
        else "That report's own originating prompt wasn't recorded -- the "
             "repository should still be identifiable from the findings "
             "below.\n\n"
    )

    # Only the bug table actually matters for a fix task -- the rest of the
    # report (executive summary, vulnerability analysis, maintainability
    # notes, appendices, ...) is prose the fixer doesn't need. Fall back to
    # the full finding if the report isn't shaped as expected (e.g. an older
    # report, or a differently-templated one) rather than silently dropping
    # content the fix task might actually need.
    bug_analysis = _extract_section(row["finding"], "Potential Bug Analysis")
    if bug_analysis is None:
        bug_analysis = row["finding"]

    return (
        f"A code analysis report (blackboard task_runs.id={row['id']}) "
        f"found the following potential bugs:\n\n{bug_analysis}\n\n"
        f"{origin}"
        "Clone the repository referenced above, verify each finding against "
        "the actual code, fix the ones that are real bugs, and open a PR "
        "with your changes.\n\n"
        "Afterwards, produce a brief report to /output/report.pdf "
        "of what you fixed, how you fixed it, and your reasoning for each "
        "fix, and publish it to the blackboard with task_type='fix-summary' "
        "and send the PDF to Discord."
    )


ROUTES: dict[str, PromptBuilder] = {
    "code-analysis-report": _code_analysis_report_to_fix_prompt,
}
