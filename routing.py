"""`topic -> follow-up-prompt-template` lookup, backed by the
`agent_blackboard.routing_rules` table.

When a `run-ai-task` run publishes a `post_type='someone_take_over'` row, the
orchestrator turns that row into the prompt for a follow-up one-shot run by
looking its `topic` up in `routing_rules` and filling the matching
`prompt_template`. Adding or changing a chain is an INSERT/UPDATE in that
table, done from `episerve_api_server`'s AI Blackboard page (or raw SQL) --
no code change, no redeploy.

Only `post_type='someone_take_over'` rows are routed. A `post_type='run_me'`
row already carries its own literal `prompt` and never reaches this module
(see `flow/orchestrator_flow.py`'s `build_prompt`). A `someone_take_over` row
whose `topic` has no *enabled* rule is left eligible rather than guessed at.

Templates use `string.Template` placeholders -- `$finding`, `$prompt`,
`$id`, `$topic` (and `$$` for a literal `$`) -- substituted with
`safe_substitute`, so an unknown `$name` or a literal `{`/`}` (JSON, code
snippets) in a template is passed through untouched rather than raising.
There is deliberately no other logic here: no section extraction, no
conditionals. Whatever a rule needs must be expressible as a flat template.
"""

import string

import pymysql  # noqa: F401  (type-only; keeps this module's import surface
                 # aligned with flow/orchestrator_flow.py)


def load_routes(conn: "pymysql.connections.Connection") -> dict[str, str]:
    """Return every enabled routing rule as a `topic -> prompt_template` dict.

    Args:
        conn: Open blackboard connection (dict-row cursor).

    Returns:
        One entry per row in `routing_rules` with `enabled` set. A `topic`
        whose only rule is disabled is absent from the dict, so it routes to
        nothing -- identical to a `topic` with no rule at all.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT topic, prompt_template FROM routing_rules WHERE enabled")
        return {r["topic"]: r["prompt_template"] for r in cur.fetchall()}


def render_prompt(template: str, row: dict) -> str:
    """Fill one rule's `prompt_template` from a `task_runs` row.

    Args:
        template: The rule's `prompt_template`.
        row: The `post_type='someone_take_over'` row being routed.

    Returns:
        `template` with `$id`, `$topic`, `$prompt` and `$finding`
        substituted. A NULL/absent column becomes an empty string. Unknown
        placeholders and literal braces are left as-is (`safe_substitute`).
    """
    return string.Template(template).safe_substitute(
        id=row.get("id", "") if row.get("id") is not None else "",
        topic=row.get("topic") or "",
        prompt=row.get("prompt") or "",
        finding=row.get("finding") or "",
    )
