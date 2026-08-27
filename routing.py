"""task_type -> prompt-builder registry, for `kind='result'` blackboard rows.

Each entry maps a `task_runs.task_type` value to a function that turns that
row's `result` into the prompt for a follow-up one-shot agent run. Add an
entry here to teach the orchestrator about a new kind of chainable result --
no other code needs to change.

Only `kind='result'` rows are routed through this registry -- a `kind='initial'`
row already carries its own literal `prompt` and bypasses this file entirely
(see `flow/orchestrator_flow.py`'s `build_prompt`). A `result` row whose
`task_type` has no entry here is left eligible rather than guessed at.
"""

from typing import Callable

PromptBuilder = Callable[[dict], str]


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
    return (
        f"A code analysis report (blackboard task_runs.id={row['id']}) found "
        f"the following issues:\n\n{row['result']}\n\n"
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
