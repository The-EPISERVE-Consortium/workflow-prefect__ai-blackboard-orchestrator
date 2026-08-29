from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from flow.orchestrator_flow import (
    MAX_RUNNING_AGE_MINUTES,
    blackboard_orchestrator,
    build_prompt,
    claim_row,
    fetch_eligible_rows,
    fetch_running_rows,
    finish_running_row,
    flow_run_logs_have_errors,
    get_flow_run_state,
    mark_running,
    reconcile_running_row,
)


_DEFAULT_ROUTES_RESULT = [
    {"topic": "code-analysis-report",
     "prompt_template": "A code analysis report found:\n\n$finding"},
]


class FakeCursor:
    """Stand-in for a pymysql DictCursor used as a context manager.

    `fetchall` is query-aware: a poll issues three SELECTs -- running rows
    (`running_result`), eligible rows (`fetchall_result`), and routing_rules
    (`routes_result`).
    """

    def __init__(self, fetchall_result=None, execute_returns=1, routes_result=None,
                 running_result=None):
        self.fetchall_result = fetchall_result or []
        self.running_result = running_result or []
        self.routes_result = _DEFAULT_ROUTES_RESULT if routes_result is None else routes_result
        self.execute_returns = execute_returns
        self.executed = []
        self._last_query = ""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        self.executed.append((query, params))
        self._last_query = query
        return self.execute_returns

    def fetchall(self):
        if "routing_rules" in self._last_query:
            return self.routes_result
        if "state = 'running'" in self._last_query:
            return self.running_result
        return self.fetchall_result


class FakeConnection:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor
        self.committed = False
        self.closed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def mock_logger():
    logger = MagicMock()
    with patch("flow.orchestrator_flow.get_run_logger", return_value=logger):
        yield logger


# ---------------------------------------------------------------------------
# dispatch pass
# ---------------------------------------------------------------------------


def test_fetch_eligible_rows_returns_cursor_rows():
    cursor = FakeCursor(fetchall_result=[{"id": 1, "post_type": "someone_take_over", "topic": "code-analysis-report", "finding": "x"}])
    conn = FakeConnection(cursor)

    rows = fetch_eligible_rows(conn)

    assert rows == [{"id": 1, "post_type": "someone_take_over", "topic": "code-analysis-report", "finding": "x"}]
    assert "state = 'waiting'" in cursor.executed[0][0]
    assert "waiting_for_next_periodic_run" in cursor.executed[0][0]


def test_claim_row_succeeds_when_one_row_affected():
    cursor = FakeCursor(execute_returns=1)
    conn = FakeConnection(cursor)

    won = claim_row(conn, row_id=5)

    assert won is True
    assert conn.committed is True
    query, params = cursor.executed[0]
    assert "state='dispatching_run'" in query
    assert params == (5,)


def test_claim_row_loses_race_when_zero_rows_affected():
    cursor = FakeCursor(execute_returns=0)
    conn = FakeConnection(cursor)

    assert claim_row(conn, row_id=5) is False


def test_mark_running_sets_state_and_flow_run_id():
    cursor = FakeCursor()
    conn = FakeConnection(cursor)

    mark_running(conn, row_id=7, flow_run_id="fr-abc")

    query, params = cursor.executed[0]
    assert "state='running'" in query
    assert "triggered_flow_run_id=%s" in query
    assert "AND state='dispatching_run'" in query
    assert params == ("fr-abc", 7)
    assert conn.committed is True


def test_mark_running_stores_null_flow_run_id_when_missing():
    cursor = FakeCursor()
    conn = FakeConnection(cursor)

    mark_running(conn, row_id=7, flow_run_id=None)

    _, params = cursor.executed[0]
    assert params == (None, 7)


def test_build_prompt_uses_literal_prompt_for_run_me_rows():
    row = {"post_type": "run_me", "prompt": "Clone <repo>, analyse it.", "topic": None}

    assert build_prompt(row, {}) == "Clone <repo>, analyse it."


def test_build_prompt_routes_someone_take_over_rows_via_routing_table():
    row = {"post_type": "someone_take_over", "topic": "code-analysis-report", "id": 1, "finding": "some finding"}

    assert "some finding" in build_prompt(row, {"code-analysis-report": "report:\n\n$finding"})


def test_build_prompt_returns_none_for_unrouted_someone_take_over_row():
    row = {"post_type": "someone_take_over", "topic": "no-such-route", "id": 2, "finding": "n/a"}

    assert build_prompt(row, {"code-analysis-report": "report:\n\n$finding"}) is None


# ---------------------------------------------------------------------------
# finish_running_row
# ---------------------------------------------------------------------------


def test_finish_running_row_ok_non_periodic_resolves():
    cursor = FakeCursor()
    conn = FakeConnection(cursor)

    finish_running_row(conn, {"id": 7, "post_type": "someone_take_over", "periodic_interval_minutes": None}, ok=True)

    query, params = cursor.executed[0]
    assert "state='resolved'" in query
    assert "AND state='running'" in query
    assert params == (7,)


def test_finish_running_row_ok_periodic_arms_cooldown():
    cursor = FakeCursor()
    conn = FakeConnection(cursor)

    finish_running_row(conn, {"id": 8, "post_type": "run_me", "periodic_interval_minutes": 60}, ok=True)

    query, params = cursor.executed[0]
    assert "state='waiting_for_next_periodic_run'" in query
    assert "periodic_last_triggered_at=%s" in query
    assert isinstance(params[0], datetime)
    assert params[-1] == 8


def test_finish_running_row_not_ok_fails():
    cursor = FakeCursor()
    conn = FakeConnection(cursor)

    finish_running_row(conn, {"id": 9, "post_type": "run_me", "periodic_interval_minutes": 60}, ok=False)

    query, params = cursor.executed[0]
    assert "state='failed'" in query
    assert params == (9,)


# ---------------------------------------------------------------------------
# Prefect REST helpers
# ---------------------------------------------------------------------------


def _resp(payload):
    return SimpleNamespace(raise_for_status=lambda: None, json=lambda: payload)


def test_get_flow_run_state_returns_state_type():
    with patch("flow.orchestrator_flow.httpx.get",
               return_value=_resp({"state": {"type": "COMPLETED"}})) as mock_get:
        assert get_flow_run_state("fr-1") == "COMPLETED"
    assert "/flow_runs/fr-1" in mock_get.call_args[0][0]


def test_flow_run_logs_have_errors_true_on_error_level():
    with patch("flow.orchestrator_flow.httpx.post",
               return_value=_resp([{"level": 20, "message": "ok"}, {"level": 40, "message": "boom"}])):
        assert flow_run_logs_have_errors("fr-1") is True


def test_flow_run_logs_have_errors_true_on_marker():
    with patch("flow.orchestrator_flow.httpx.post",
               return_value=_resp([{"level": 20, "message": "Traceback (most recent call last):"}])):
        assert flow_run_logs_have_errors("fr-1") is True


def test_flow_run_logs_have_errors_false_when_clean():
    with patch("flow.orchestrator_flow.httpx.post",
               return_value=_resp([{"level": 20, "message": "[done] agent_settled"}])):
        assert flow_run_logs_have_errors("fr-1") is False


def test_flow_run_logs_have_errors_false_when_no_logs():
    with patch("flow.orchestrator_flow.httpx.post", return_value=_resp([])):
        assert flow_run_logs_have_errors("fr-1") is False


# ---------------------------------------------------------------------------
# reconcile_running_row
# ---------------------------------------------------------------------------


def _running_row(**over):
    row = {
        "id": 1, "post_type": "someone_take_over", "periodic_interval_minutes": None,
        "triggered_flow_run_id": "fr-1",
        "last_state_change": datetime.now(timezone.utc) - timedelta(minutes=5),
    }
    row.update(over)
    return row


def test_reconcile_completed_clean_resolves(mock_logger):
    cursor = FakeCursor()
    conn = FakeConnection(cursor)

    with patch("flow.orchestrator_flow.get_flow_run_state", return_value="COMPLETED"), \
         patch("flow.orchestrator_flow.flow_run_logs_have_errors", return_value=False):
        reconcile_running_row(conn, _running_row(), mock_logger)

    assert any("state='resolved'" in q for q, _ in cursor.executed)


def test_reconcile_completed_with_log_errors_fails(mock_logger):
    cursor = FakeCursor()
    conn = FakeConnection(cursor)

    with patch("flow.orchestrator_flow.get_flow_run_state", return_value="COMPLETED"), \
         patch("flow.orchestrator_flow.flow_run_logs_have_errors", return_value=True):
        reconcile_running_row(conn, _running_row(), mock_logger)

    assert any("state='failed'" in q for q, _ in cursor.executed)


@pytest.mark.parametrize("bad_state", ["FAILED", "CRASHED", "CANCELLED"])
def test_reconcile_terminal_bad_fails(mock_logger, bad_state):
    cursor = FakeCursor()
    conn = FakeConnection(cursor)

    with patch("flow.orchestrator_flow.get_flow_run_state", return_value=bad_state):
        reconcile_running_row(conn, _running_row(), mock_logger)

    assert any("state='failed'" in q for q, _ in cursor.executed)


def test_reconcile_non_terminal_young_leaves_row(mock_logger):
    cursor = FakeCursor()
    conn = FakeConnection(cursor)

    with patch("flow.orchestrator_flow.get_flow_run_state", return_value="RUNNING"):
        reconcile_running_row(conn, _running_row(), mock_logger)

    assert cursor.executed == []


def test_reconcile_non_terminal_but_too_old_fails(mock_logger):
    cursor = FakeCursor()
    conn = FakeConnection(cursor)
    old = datetime.now(timezone.utc) - timedelta(minutes=MAX_RUNNING_AGE_MINUTES + 10)

    with patch("flow.orchestrator_flow.get_flow_run_state", return_value="RUNNING"):
        reconcile_running_row(conn, _running_row(last_state_change=old), mock_logger)

    assert any("state='failed'" in q for q, _ in cursor.executed)


def test_reconcile_prefect_unreadable_leaves_young_row(mock_logger):
    cursor = FakeCursor()
    conn = FakeConnection(cursor)

    with patch("flow.orchestrator_flow.get_flow_run_state", side_effect=RuntimeError("boom")):
        reconcile_running_row(conn, _running_row(), mock_logger)

    assert cursor.executed == []


def test_reconcile_no_flow_run_id_ages_out(mock_logger):
    cursor = FakeCursor()
    conn = FakeConnection(cursor)
    old = datetime.now(timezone.utc) - timedelta(minutes=MAX_RUNNING_AGE_MINUTES + 10)

    reconcile_running_row(conn, _running_row(triggered_flow_run_id=None, last_state_change=old), mock_logger)

    assert any("state='failed'" in q for q, _ in cursor.executed)


# ---------------------------------------------------------------------------
# blackboard_orchestrator: dispatch pass ends at 'running'
# ---------------------------------------------------------------------------


def test_orchestrator_dispatches_someone_take_over_row_to_running():
    rows = [{"id": 1, "post_type": "someone_take_over", "topic": "code-analysis-report", "finding": "some finding"}]
    cursor = FakeCursor(fetchall_result=rows, execute_returns=1)
    conn = FakeConnection(cursor)

    with patch("flow.orchestrator_flow._connect", return_value=conn), \
         patch("flow.orchestrator_flow.run_deployment",
               return_value=SimpleNamespace(id="fr-1234")) as mock_run_deployment:
        blackboard_orchestrator.fn()

    mock_run_deployment.assert_called_once()
    _, kwargs = mock_run_deployment.call_args
    assert kwargs["name"] == "agent-task-pipeline/manual"
    assert "some finding" in kwargs["parameters"]["prompt"]
    assert kwargs["timeout"] == 0

    queries = [q for q, _ in cursor.executed]
    assert any("state='dispatching_run'" in q for q in queries)
    assert any("state='running'" in q for q in queries)
    assert not any("state='resolved'" in q for q in queries)  # not until reconcile
    running_update = next((p for q, p in cursor.executed if "state='running'" in q and "UPDATE" in q), None)
    assert running_update is not None and "fr-1234" in running_update
    assert conn.closed is True


def test_orchestrator_skips_unknown_topic():
    rows = [{"id": 2, "post_type": "someone_take_over", "topic": "no-such-route", "finding": "n/a"}]
    cursor = FakeCursor(fetchall_result=rows)
    conn = FakeConnection(cursor)

    with patch("flow.orchestrator_flow._connect", return_value=conn), \
         patch("flow.orchestrator_flow.run_deployment") as mock_run_deployment:
        blackboard_orchestrator.fn()

    mock_run_deployment.assert_not_called()
    # three SELECTs ran (running rows + eligible rows + routing_rules), no UPDATEs
    assert len(cursor.executed) == 3
    assert all("SELECT" in q for q, _ in cursor.executed)


def test_orchestrator_skips_row_lost_to_another_claim():
    rows = [{"id": 3, "post_type": "someone_take_over", "topic": "code-analysis-report", "finding": "x"}]
    cursor = FakeCursor(fetchall_result=rows, execute_returns=0)  # claim UPDATE affects 0 rows
    conn = FakeConnection(cursor)

    with patch("flow.orchestrator_flow._connect", return_value=conn), \
         patch("flow.orchestrator_flow.run_deployment") as mock_run_deployment:
        blackboard_orchestrator.fn()

    mock_run_deployment.assert_not_called()


def test_orchestrator_dispatches_run_me_to_running_regardless_of_periodic():
    rows = [{
        "id": 5, "post_type": "run_me", "topic": None, "prompt": "Clone <repo>, do Y.",
        "periodic_interval_minutes": 60,
    }]
    cursor = FakeCursor(fetchall_result=rows, execute_returns=1)
    conn = FakeConnection(cursor)

    with patch("flow.orchestrator_flow._connect", return_value=conn), \
         patch("flow.orchestrator_flow.run_deployment",
               return_value=SimpleNamespace(id="fr-periodic")):
        blackboard_orchestrator.fn()

    queries = [q for q, _ in cursor.executed]
    assert any("state='running'" in q for q in queries)
    # dispatch no longer arms the cooldown -- that happens at reconcile
    assert not any("SET state='waiting_for_next_periodic_run'" in q for q in queries)


def test_orchestrator_reconciles_running_rows_before_dispatch():
    running = [{
        "id": 9, "post_type": "someone_take_over", "periodic_interval_minutes": None,
        "triggered_flow_run_id": "fr-9",
        "last_state_change": datetime.now(timezone.utc) - timedelta(minutes=2),
    }]
    cursor = FakeCursor(fetchall_result=[], running_result=running, execute_returns=1)
    conn = FakeConnection(cursor)

    with patch("flow.orchestrator_flow._connect", return_value=conn), \
         patch("flow.orchestrator_flow.run_deployment") as mock_run_deployment, \
         patch("flow.orchestrator_flow.get_flow_run_state", return_value="COMPLETED"), \
         patch("flow.orchestrator_flow.flow_run_logs_have_errors", return_value=False):
        blackboard_orchestrator.fn()

    mock_run_deployment.assert_not_called()
    assert any("state='resolved'" in q for q, _ in cursor.executed)
