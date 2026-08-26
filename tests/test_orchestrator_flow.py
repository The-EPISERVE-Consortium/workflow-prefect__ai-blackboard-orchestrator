from unittest.mock import MagicMock, patch

import pytest

from flow.orchestrator_flow import blackboard_orchestrator, claim_row, fetch_new_rows, mark_done


class FakeCursor:
    """Stand-in for a pymysql DictCursor used as a context manager."""

    def __init__(self, fetchall_result=None, execute_returns=1):
        self.fetchall_result = fetchall_result or []
        self.execute_returns = execute_returns
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        self.executed.append((query, params))
        return self.execute_returns

    def fetchall(self):
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


def test_fetch_new_rows_returns_cursor_rows():
    cursor = FakeCursor(fetchall_result=[{"id": 1, "task_type": "bug-report", "result": "x"}])
    conn = FakeConnection(cursor)

    rows = fetch_new_rows(conn)

    assert rows == [{"id": 1, "task_type": "bug-report", "result": "x"}]
    assert "status='new'" in cursor.executed[0][0]


def test_claim_row_succeeds_when_one_row_affected():
    cursor = FakeCursor(execute_returns=1)
    conn = FakeConnection(cursor)

    won = claim_row(conn, row_id=5, claimed_by="run-1")

    assert won is True
    assert conn.committed is True
    query, params = cursor.executed[0]
    assert "status='claimed'" in query
    assert params[0] == "run-1"
    assert params[2] == 5


def test_claim_row_loses_race_when_zero_rows_affected():
    cursor = FakeCursor(execute_returns=0)
    conn = FakeConnection(cursor)

    won = claim_row(conn, row_id=5, claimed_by="run-1")

    assert won is False


def test_mark_done_updates_status():
    cursor = FakeCursor()
    conn = FakeConnection(cursor)

    mark_done(conn, row_id=7)

    query, params = cursor.executed[0]
    assert "status='done'" in query
    assert params == (7,)
    assert conn.committed is True


def test_orchestrator_triggers_follow_up_for_known_task_type():
    rows = [{"id": 1, "task_type": "bug-report", "result": "some finding"}]
    cursor = FakeCursor(fetchall_result=rows, execute_returns=1)
    conn = FakeConnection(cursor)

    with patch("flow.orchestrator_flow._connect", return_value=conn), \
         patch("flow.orchestrator_flow.run_deployment") as mock_run_deployment:
        blackboard_orchestrator.fn()

    mock_run_deployment.assert_called_once()
    _, kwargs = mock_run_deployment.call_args
    assert kwargs["name"] == "agent-task-pipeline/manual"
    assert "some finding" in kwargs["parameters"]["prompt"]
    assert kwargs["timeout"] == 0
    # claim (UPDATE ... claimed) then done (UPDATE ... done)
    queries = [q for q, _ in cursor.executed[1:]]
    assert any("status='claimed'" in q for q in queries)
    assert any("status='done'" in q for q in queries)
    assert conn.closed is True


def test_orchestrator_skips_unknown_task_type():
    rows = [{"id": 2, "task_type": "no-such-route", "result": "n/a"}]
    cursor = FakeCursor(fetchall_result=rows)
    conn = FakeConnection(cursor)

    with patch("flow.orchestrator_flow._connect", return_value=conn), \
         patch("flow.orchestrator_flow.run_deployment") as mock_run_deployment:
        blackboard_orchestrator.fn()

    mock_run_deployment.assert_not_called()
    # only the initial SELECT ran -- no claim/done UPDATEs for the unrouted row
    assert len(cursor.executed) == 1


def test_orchestrator_skips_row_lost_to_another_claim():
    rows = [{"id": 3, "task_type": "bug-report", "result": "x"}]
    cursor = FakeCursor(fetchall_result=rows, execute_returns=0)  # claim UPDATE affects 0 rows
    conn = FakeConnection(cursor)

    with patch("flow.orchestrator_flow._connect", return_value=conn), \
         patch("flow.orchestrator_flow.run_deployment") as mock_run_deployment:
        blackboard_orchestrator.fn()

    mock_run_deployment.assert_not_called()
