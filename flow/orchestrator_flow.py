"""Polls the shared blackboard (`agent_blackboard.task_runs` in MariaDB) and
drives each row through its lifecycle:

- `post_type='someone_take_over'` -- written by a completed `run-ai-task` run
  via its `blackboard-communication` skill. Has a `finding` payload; the
  follow-up prompt is built by filling the `prompt_template` of the matching
  `agent_blackboard.routing_rules` row, keyed on `topic` (see `routing.py`).
- `post_type='run_me'` -- seeded directly with its own `prompt`, no
  `finding`. Recurs every `periodic_interval_minutes` if set, otherwise fires
  once.

Each poll does two passes:

1. **Reconcile** -- for every row already `state='running'`, read the Prefect
   flow run it triggered (`triggered_flow_run_id`). If that run has finished,
   advance the row: `COMPLETED` with no error log and the agent's
   `AGENT_DONE_MARKER` present -> `resolved` (or
   `waiting_for_next_periodic_run` for a periodic `run_me`, arming its
   cooldown); `FAILED`/`CRASHED`/`CANCELLED`, `COMPLETED` with an error in
   its logs, or `COMPLETED` without the done marker (crashed / cut off
   mid-task) -> `failed`. A row stuck `running` past
   `MAX_RUNNING_AGE_MINUTES` -> `failed`.
2. **Dispatch** -- for every eligible row, build a prompt, atomically claim
   it, `run_deployment()` the `manual` deployment in `run-ai-task`, and set
   the row to `running` (recording the triggered run's id).

`failed` rows are terminal until a human re-queues them (the AI Blackboard
page's "Set to waiting"). Failure detail is not stored on the row -- read it
from the Prefect run (the row keeps a link via `triggered_flow_run_id`) and
this flow's own logs.
"""

import os
from datetime import datetime, timezone

import httpx
import pymysql
import pymysql.cursors
from prefect import flow, get_run_logger
from prefect.deployments import run_deployment

from routing import load_routes, render_prompt

MANUAL_DEPLOYMENT = "agent-task-pipeline/manual"

# A row is eligible to *dispatch* either because it's never been dispatched
# ('waiting'), or because it's a periodic row whose cooldown has elapsed since
# its last run completed. Shared verbatim between the SELECT and the per-row
# claiming UPDATE so a row can't be double-claimed by two overlapping
# orchestrator runs (the UPDATE only ever affects the row still matching it).
ELIGIBILITY_CLAUSE = (
    "state = 'waiting' OR ("
    "state = 'waiting_for_next_periodic_run' "
    "AND periodic_last_triggered_at < NOW() - INTERVAL periodic_interval_minutes MINUTE"
    ")"
)

# Prefect flow-run state types.
_TERMINAL_OK = {"COMPLETED"}
_TERMINAL_BAD = {"FAILED", "CRASHED", "CANCELLED"}

_ERROR_LOG_LEVEL = 40  # logging.ERROR

# Prefect's POST /logs/filter caps `limit` at 200; scan at most this many of
# the run's most-recent records for markers.
_LOG_SCAN_LIMIT = 200
_LOG_SCAN_MAX_RECORDS = 1000

# A 'running' row whose Prefect run never reaches a terminal state (hung
# agent, lost run, unreadable id) is forced to 'failed' once it's been
# running this long.
MAX_RUNNING_AGE_MINUTES = int(os.environ.get("MAX_RUNNING_AGE_MINUTES", "360"))

# On a COMPLETED run, fail the row if any log record is at ERROR+ level or a
# message contains one of these substrings. Tunable via LOG_ERROR_MARKERS
# (comma-separated).
LOG_ERROR_MARKERS = [
    m.strip()
    for m in os.environ.get(
        "LOG_ERROR_MARKERS", "Traceback (most recent call last),[tool_result:ERROR]"
    ).split(",")
    if m.strip()
]

# The line the harness-conventions skill tells the agent to emit as the last
# thing it outputs, only when the whole task actually succeeded. A COMPLETED
# run whose logs don't contain this is treated as 'incomplete' -> failed.
AGENT_DONE_MARKER = os.environ.get("AGENT_DONE_MARKER", "===AGENT_TASKS_COMPLETE===")


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


# ---------------------------------------------------------------------------
# Prefect REST reads (raw httpx -- in-cluster self-hosted Prefect needs no
# auth; mirrors episerve_api_server/app/clients/prefect.py). Both raise on
# transport / HTTP / shape errors; callers treat that as "can't tell yet".
# ---------------------------------------------------------------------------


def get_flow_run_state(flow_run_id: str) -> str:
    """Return the Prefect state type for a flow run.

    Args:
        flow_run_id: The Prefect flow run's UUID.

    Returns:
        One of Prefect's state types, e.g. 'SCHEDULED', 'RUNNING',
        'COMPLETED', 'FAILED', 'CRASHED', 'CANCELLED'.
    """
    base = os.environ["PREFECT_API_URL"].rstrip("/")
    resp = httpx.get(f"{base}/flow_runs/{flow_run_id}", timeout=15)
    resp.raise_for_status()
    return resp.json()["state"]["type"]


def _iter_recent_logs(flow_run_id: str):
    """Yield a COMPLETED run's log records, most-recent first, up to
    `_LOG_SCAN_MAX_RECORDS` (`/logs/filter` has no message-substring filter,
    so message scanning is client-side)."""
    base = os.environ["PREFECT_API_URL"].rstrip("/")
    yielded = 0
    while yielded < _LOG_SCAN_MAX_RECORDS:
        resp = httpx.post(
            f"{base}/logs/filter",
            json={
                "logs": {"flow_run_id": {"any_": [flow_run_id]}},
                "sort": "TIMESTAMP_DESC",
                "limit": _LOG_SCAN_LIMIT,
                "offset": yielded,
            },
            timeout=15,
        )
        resp.raise_for_status()
        page = resp.json()
        for rec in page:
            yield rec
        yielded += len(page)
        if len(page) < _LOG_SCAN_LIMIT:
            return


def flow_run_outcome(flow_run_id: str) -> str:
    """Classify a Prefect-COMPLETED run from its logs.

    Returns:
        'error'      -- an ERROR+ log record (checked server-side, position
                        independent), or a `LOG_ERROR_MARKERS` substring.
        'incomplete' -- no error, but the run never logged `AGENT_DONE_MARKER`
                        (crashed / cut off mid-task, or the agent judged it
                        unfinished).
        'clean'      -- no error and the done marker is present.
    """
    base = os.environ["PREFECT_API_URL"].rstrip("/")

    resp = httpx.post(
        f"{base}/logs/filter",
        json={
            "logs": {"flow_run_id": {"any_": [flow_run_id]}, "level": {"ge_": _ERROR_LOG_LEVEL}},
            "limit": 1,
        },
        timeout=15,
    )
    resp.raise_for_status()
    if resp.json():
        return "error"

    saw_done = False
    for rec in _iter_recent_logs(flow_run_id):
        message = rec.get("message") or ""
        if any(marker in message for marker in LOG_ERROR_MARKERS):
            return "error"
        if AGENT_DONE_MARKER in message:
            saw_done = True
    return "clean" if saw_done else "incomplete"


# ---------------------------------------------------------------------------
# Pass 1: reconcile in-flight runs
# ---------------------------------------------------------------------------


def fetch_running_rows(conn: pymysql.connections.Connection) -> list[dict]:
    """Return every row currently `state='running'`, oldest transition first."""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM task_runs WHERE state = 'running' ORDER BY last_state_change")
        return list(cur.fetchall())


def _minutes_since(value) -> float | None:
    """Minutes elapsed since `value` (a datetime, or a 'YYYY-MM-DD HH:MM:SS'
    string), or None if it can't be parsed."""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = datetime.strptime(value[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    now = datetime.now(value.tzinfo) if value.tzinfo else datetime.now()
    return (now - value).total_seconds() / 60


def finish_running_row(conn: pymysql.connections.Connection, row: dict, ok: bool) -> None:
    """Move a reconciled `running` row to its resting state.

    - ok + periodic `run_me`  -> `waiting_for_next_periodic_run`, cooldown
      armed now (so a periodic task can't overlap itself).
    - ok                      -> `resolved`.
    - not ok                  -> `failed` (terminal until a human re-queues).

    The `AND state='running'` guard makes this a no-op if the row was moved
    (dismissed, or reconciled by an overlapping poll) since it was read.
    """
    periodic = row["post_type"] == "run_me" and row.get("periodic_interval_minutes") is not None
    with conn.cursor() as cur:
        if ok and periodic:
            cur.execute(
                "UPDATE task_runs SET state='waiting_for_next_periodic_run', "
                "periodic_last_triggered_at=%s WHERE id=%s AND state='running'",
                (datetime.now(timezone.utc), row["id"]),
            )
        elif ok:
            cur.execute(
                "UPDATE task_runs SET state='resolved' WHERE id=%s AND state='running'",
                (row["id"],),
            )
        else:
            cur.execute(
                "UPDATE task_runs SET state='failed' WHERE id=%s AND state='running'",
                (row["id"],),
            )
        conn.commit()


def reconcile_running_row(conn: pymysql.connections.Connection, row: dict, logger) -> None:
    """Check one `running` row's Prefect run and advance it if it has finished.

    Non-terminal, or any error reading Prefect, leaves the row `running` for
    the next poll -- unless it's been `running` past MAX_RUNNING_AGE_MINUTES,
    in which case it's forced to `failed`.
    """
    row_id = row["id"]
    fid = row.get("triggered_flow_run_id")

    state = None
    if fid:
        try:
            state = get_flow_run_state(fid)
        except Exception as exc:
            logger.warning(f"id={row_id}: could not read Prefect state for {fid}: {exc}")
            return  # leave running, retry next poll

    if state in _TERMINAL_OK:
        try:
            outcome = flow_run_outcome(fid)
        except Exception as exc:
            logger.warning(f"id={row_id}: could not read logs for {fid} ({exc}); treating as clean")
            outcome = "clean"
        logger.info(f"id={row_id}: run {fid} COMPLETED, outcome={outcome}")
        finish_running_row(conn, row, ok=(outcome == "clean"))
        return

    if state in _TERMINAL_BAD:
        logger.info(f"id={row_id}: run {fid} {state} -> failed")
        finish_running_row(conn, row, ok=False)
        return

    age = _minutes_since(row.get("last_state_change"))
    if age is not None and age > MAX_RUNNING_AGE_MINUTES:
        logger.warning(
            f"id={row_id}: running {age:.0f} min (Prefect state={state or 'unknown'}), "
            f"no terminal state -> failed"
        )
        finish_running_row(conn, row, ok=False)
    # else: still running, leave it


# ---------------------------------------------------------------------------
# Pass 2: dispatch eligible rows
# ---------------------------------------------------------------------------


def fetch_eligible_rows(conn: pymysql.connections.Connection) -> list[dict]:
    """Return every blackboard row currently eligible to be dispatched.

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

    Returns:
        `row['prompt']` verbatim for a `post_type='run_me'` row; the matching
        rule's `prompt_template` filled from `row` (via
        `routing.render_prompt`) for a `post_type='someone_take_over'` row
        whose `topic` has an enabled rule; None if there's no rule.
    """
    if row["post_type"] == "run_me":
        return row["prompt"]
    template = routes.get(row["topic"])
    return render_prompt(template, row) if template is not None else None


def mark_running(
    conn: pymysql.connections.Connection, row_id: int, flow_run_id: str | None
) -> None:
    """Mark a just-dispatched row `running`, recording the triggered flow run.

    The row stays `running` until a later poll's reconcile pass reads the
    Prefect run's outcome (see `reconcile_running_row`) and advances it to
    `resolved` / `waiting_for_next_periodic_run` / `failed`.

    The `AND state='dispatching_run'` guard means only the poll that just
    claimed this row can move it on.

    Args:
        conn: Open blackboard connection.
        row_id: `task_runs.id` to update.
        flow_run_id: The triggered Prefect flow run's id, or None if
            `run_deployment` didn't hand one back.
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE task_runs SET state='running', triggered_flow_run_id=%s "
            "WHERE id=%s AND state='dispatching_run'",
            (flow_run_id, row_id),
        )
        conn.commit()


@flow
def blackboard_orchestrator() -> None:
    """Poll the blackboard once: reconcile in-flight runs, then dispatch."""
    logger = get_run_logger()

    conn = _connect()
    try:
        # Pass 1: reconcile running rows first, so a periodic row whose run
        # just completed can be re-dispatched in this same poll.
        running = fetch_running_rows(conn)
        logger.info(f"reconciling {len(running)} running row(s)")
        for row in running:
            reconcile_running_row(conn, row, logger)

        # Pass 2: dispatch newly-eligible rows.
        rows = fetch_eligible_rows(conn)
        logger.info(f"found {len(rows)} eligible row(s)")

        routes = load_routes(conn)

        for row in rows:
            prompt = build_prompt(row, routes)
            if prompt is None:
                logger.info(f"no route for topic={row['topic']!r}, leaving id={row['id']} as-is")
                continue

            if not claim_row(conn, row["id"]):
                logger.info(f"id={row['id']} claimed by another run first, skipping")
                continue

            # timeout=0 -> returns immediately with the created (SCHEDULED)
            # FlowRun; we only want its id, not to wait for it.
            flow_run = run_deployment(name=MANUAL_DEPLOYMENT, parameters={"prompt": prompt}, timeout=0)
            flow_run_id = str(flow_run.id) if flow_run is not None else None
            logger.info(
                f"triggered {MANUAL_DEPLOYMENT} for task_runs.id={row['id']} "
                f"(post_type={row['post_type']}, flow_run_id={flow_run_id})"
            )

            mark_running(conn, row["id"], flow_run_id)
    finally:
        conn.close()
