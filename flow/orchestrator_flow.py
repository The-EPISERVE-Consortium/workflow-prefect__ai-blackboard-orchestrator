"""Polls the shared blackboard (`agent_blackboard.task_runs` in MariaDB) for
new results from one-shot agent runs, and triggers a follow-up one-shot run
for any it recognizes.

This is the read/trigger side of a blackboard-based chaining pattern --
`workflow-prefect__run-ai-task`'s `blackboard-communication` skill is the
write side (a task publishes its result there only if its prompt explicitly
asks for it). The two repos are deliberately decoupled: this flow doesn't
know or care which task produced a row, only what `task_type` it's tagged
with (see routing.py), and `run-ai-task` has no knowledge that anything
downstream is polling its output.

No Kubernetes/subprocess orchestration here -- triggering a follow-up run is
just `run_deployment()` against the `manual` deployment already registered
in `run-ai-task` (see its deploy/deployer.py), the same one-off-prompt path
`run-ai-task/run.py` and a human both already use.
"""

import os
from datetime import datetime, timezone

import pymysql
import pymysql.cursors
from prefect import flow, get_run_logger
from prefect.deployments import run_deployment
from prefect.runtime import flow_run

from routing import ROUTES

MANUAL_DEPLOYMENT = "agent-task-pipeline/manual"


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
                            # result/trace routinely contain 4-byte characters
    )


def fetch_new_rows(conn: pymysql.connections.Connection) -> list[dict]:
    """Return every blackboard row still awaiting a routing decision.

    Args:
        conn: Open blackboard connection.

    Returns:
        Rows with status='new', oldest first.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM task_runs WHERE status='new' ORDER BY created_at")
        return list(cur.fetchall())


def claim_row(conn: pymysql.connections.Connection, row_id: int, claimed_by: str) -> bool:
    """Atomically claim one row before acting on it.

    The `WHERE status='new'` guard makes this safe against two overlapping
    orchestrator runs racing on the same row -- only one `UPDATE` can match.

    Args:
        conn: Open blackboard connection.
        row_id: `task_runs.id` to claim.
        claimed_by: Identifier for this orchestrator run (its own flow run id).

    Returns:
        True if this call won the claim, False if the row was no longer 'new'.
    """
    with conn.cursor() as cur:
        affected = cur.execute(
            "UPDATE task_runs SET status='claimed', claimed_by=%s, claimed_at=%s "
            "WHERE id=%s AND status='new'",
            (claimed_by, datetime.now(timezone.utc), row_id),
        )
        conn.commit()
        return affected == 1


def mark_done(conn: pymysql.connections.Connection, row_id: int) -> None:
    """Mark a claimed row done once its follow-up run has been triggered.

    Note this means "handed off to a new run", not "the follow-up run
    succeeded" -- there is no feedback path from the triggered run back to
    this row. If the follow-up itself needs to report a result, it publishes
    its own new row via the blackboard-communication skill.

    Args:
        conn: Open blackboard connection.
        row_id: `task_runs.id` to mark done.
    """
    with conn.cursor() as cur:
        cur.execute("UPDATE task_runs SET status='done' WHERE id=%s", (row_id,))
        conn.commit()


@flow
def blackboard_orchestrator() -> None:
    """Poll the blackboard once, triggering a follow-up run per actionable row."""
    logger = get_run_logger()
    claimed_by = str(flow_run.id)

    conn = _connect()
    try:
        rows = fetch_new_rows(conn)
        logger.info(f"found {len(rows)} row(s) with status='new'")

        for row in rows:
            build_prompt = ROUTES.get(row["task_type"])
            if build_prompt is None:
                logger.info(f"no route for task_type={row['task_type']!r}, leaving id={row['id']} as 'new'")
                continue

            if not claim_row(conn, row["id"], claimed_by):
                logger.info(f"id={row['id']} claimed by another run first, skipping")
                continue

            prompt = build_prompt(row)
            run_deployment(name=MANUAL_DEPLOYMENT, parameters={"prompt": prompt}, timeout=0)
            logger.info(f"triggered {MANUAL_DEPLOYMENT} for task_runs.id={row['id']} (task_type={row['task_type']})")

            mark_done(conn, row["id"])
    finally:
        conn.close()
