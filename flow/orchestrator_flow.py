"""Polls the shared blackboard (`agent_blackboard.task_runs` in MariaDB) for
rows that need a follow-up `run-ai-task` run triggered, and triggers it.

Every row in `task_runs` falls into one of two `post_type`s:

- `post_type='someone_take_over'` -- written by a completed `run-ai-task` run
  via its `blackboard-communication` skill. Has a `finding` payload; this
  flow builds a follow-up prompt from it by filling the `prompt_template` of
  the matching `agent_blackboard.routing_rules` row, keyed on `task_type`
  (see `routing.py`).
- `post_type='run_me'` -- seeded directly with its own `prompt`, no
  `finding`. Recurs every `periodic_interval_minutes` if set (tracked via
  `periodic_last_triggered_at`), otherwise fires once.

Regardless of post_type, once this flow has a prompt for a row, the action is
identical: `run_deployment()` against the `manual` deployment already
registered in `run-ai-task` (see its deploy/deploy_registry.py) -- the same
one-off-prompt path `run-ai-task/deploy.py` and a human both already use. No
Kubernetes/subprocess orchestration here.
"""

import os
from datetime import datetime, timezone

import pymysql
import pymysql.cursors
from prefect import flow, get_run_logger
from prefect.deployments import run_deployment

from routing import load_routes, render_prompt

MANUAL_DEPLOYMENT = "agent-task-pipeline/manual"

# A row is eligible either because it's never been dispatched ('waiting'), or
# because it's a periodic row whose cooldown has elapsed since its last
# dispatch. Shared verbatim between the SELECT and the per-row claiming
# UPDATE so a row can't be double-claimed by two overlapping orchestrator
# runs (the UPDATE only ever affects the row still matching this clause).
ELIGIBILITY_CLAUSE = (
    "state = 'waiting' OR ("
    "state = 'waiting_for_next_periodic_run' "
    "AND periodic_last_triggered_at < NOW() - INTERVAL periodic_interval_minutes MINUTE"
    ")"
)


def _connect() -> pymysql.connections.Connection:
    """Open a connection to the blackboard database.

    Returns:
        A pymysql connection with dict-row cursors, scoped to
        `agent_blackboard` via the `blackboard` DB user (SELECT/INSERT/UPDATE
        only, no DELETE, no access to any other database on the instance).
    """
    return pymysql.connect(
        host=os.environ["MARIADB_HOST"],
        user=os.environ["BLACKBOARD_USER"],
        password=os.environ["BLACKBOARD_PASSWORD"],
        database=os.environ["BLACKBOARD_DB"],
        cursorclass=pymysql.cursors.DictCursor,
        charset="utf8mb4",  # server default connection charset is utf8mb3;
                            # finding/trace routinely contain 4-byte characters
    )


def fetch_eligible_rows(conn: pymysql.connections.Connection) -> list[dict]:
    """Return every blackboard row currently eligible to be dispatched.

    Args:
        conn: Open blackboard connection.

    Returns:
        Rows matching ELIGIBILITY_CLAUSE, oldest first.
    """
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM task_runs WHERE {ELIGIBILITY_CLAUSE} ORDER BY created_at")
        return list(cur.fetchall())


def claim_row(conn: pymysql.connections.Connection, row_id: int) -> bool:
    """Atomically claim one row before acting on it.

    Re-checking ELIGIBILITY_CLAUSE in the UPDATE's WHERE makes this safe
    against two overlapping orchestrator runs racing on the same row -- only
    one UPDATE can match.

    Args:
        conn: Open blackboard connection.
        row_id: `task_runs.id` to claim.

    Returns:
        True if this call won the claim, False if the row was no longer
        eligible.
    """
    with conn.cursor() as cur:
        affected = cur.execute(
            f"UPDATE task_runs SET state='dispatching_run' WHERE id=%s AND ({ELIGIBILITY_CLAUSE})",
            (row_id,),
        )
        conn.commit()
        return affected == 1


def build_prompt(row: dict, routes: dict[str, str]) -> str | None:
    """Return the prompt to trigger for this row, or None if it can't be built.

    Args:
        row: A `task_runs` row.
        routes: `task_type -> prompt_template`, as returned by
            `routing.load_routes` (enabled rules only).

    Returns:
        `row['prompt']` verbatim for a `post_type='run_me'` row; the matching
        rule's `prompt_template` filled from `row` (via
        `routing.render_prompt`) for a `post_type='someone_take_over'` row
        whose `task_type` has an enabled rule; None if there's no rule.
    """
    if row["post_type"] == "run_me":
        return row["prompt"]
    template = routes.get(row["task_type"])
    return render_prompt(template, row) if template is not None else None


def mark_dispatched(conn: pymysql.connections.Connection, row_id: int, periodic: bool) -> None:
    """Mark a claimed row as handled once its run has been triggered.

    A periodic row goes to 'waiting_for_next_periodic_run' with a fresh
    `periodic_last_triggered_at`, so ELIGIBILITY_CLAUSE picks it up again once
    its interval elapses. Every other row goes straight to 'resolved', which
    is terminal -- "handed off to a new run", not "the follow-up run
    succeeded"; there's no feedback path back to this row.

    Args:
        conn: Open blackboard connection.
        row_id: `task_runs.id` to update.
        periodic: Whether this row recurs (`post_type='run_me'` with
            `periodic_interval_minutes` set).
    """
    with conn.cursor() as cur:
        if periodic:
            cur.execute(
                "UPDATE task_runs SET state='waiting_for_next_periodic_run', "
                "periodic_last_triggered_at=%s WHERE id=%s",
                (datetime.now(timezone.utc), row_id),
            )
        else:
            cur.execute("UPDATE task_runs SET state='resolved' WHERE id=%s", (row_id,))
        conn.commit()


@flow
def blackboard_orchestrator() -> None:
    """Poll the blackboard once, triggering a run for each eligible row."""
    logger = get_run_logger()

    conn = _connect()
    try:
        rows = fetch_eligible_rows(conn)
        logger.info(f"found {len(rows)} eligible row(s)")

        routes = load_routes(conn)

        for row in rows:
            prompt = build_prompt(row, routes)
            if prompt is None:
                logger.info(f"no route for task_type={row['task_type']!r}, leaving id={row['id']} as-is")
                continue

            if not claim_row(conn, row["id"]):
                logger.info(f"id={row['id']} claimed by another run first, skipping")
                continue

            run_deployment(name=MANUAL_DEPLOYMENT, parameters={"prompt": prompt}, timeout=0)
            logger.info(f"triggered {MANUAL_DEPLOYMENT} for task_runs.id={row['id']} (post_type={row['post_type']})")

            periodic = row["post_type"] == "run_me" and row.get("periodic_interval_minutes") is not None
            mark_dispatched(conn, row["id"], periodic)
    finally:
        conn.close()
