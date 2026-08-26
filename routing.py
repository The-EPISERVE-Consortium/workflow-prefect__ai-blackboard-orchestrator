"""task_type -> prompt-builder registry.

This is the only place that encodes what "actionable" means for the
blackboard. Each entry maps a `task_runs.task_type` value to a function that
turns that row into the prompt for the follow-up one-shot agent run. Add an
entry here to teach the orchestrator about a new kind of chainable result --
no other code needs to change.

A row whose task_type has no entry here is left alone (still `status='new'`)
rather than guessed at -- see flow/orchestrator_flow.py.
"""

from typing import Callable

PromptBuilder = Callable[[dict], str]


def _bug_report_to_fix_prompt(row: dict) -> str:
    return (
        f"A code analysis report (blackboard task_runs.id={row['id']}) found "
        f"the following issues:\n\n{row['result']}\n\n"
        "Clone the repository referenced above, verify each finding against "
        "the actual code, and fix the ones that are real bugs. Open a PR "
        "with your changes."
    )


ROUTES: dict[str, PromptBuilder] = {
    "bug-report": _bug_report_to_fix_prompt,
}
