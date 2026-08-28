from unittest.mock import MagicMock, patch

import pytest

from flow.orchestrator_flow import (
    blackboard_orchestrator,
    build_prompt,
    claim_row,
    fetch_eligible_rows,
    mark_dispatched,
)


_DEFAULT_ROUTES_RESULT = [
    {"task_type": "code-analysis-report",
     "prompt_template": "A code analysis report found:\n\n$finding"},
]


class FakeCursor:
    """Stand-in for a pymysql DictCursor used as a context manager.

    `fetchall` is query-aware: the flow now issues two SELECTs per poll --
    one against `routing_rules` (answered with `routes_result`) and one
    against `task_runs` (answered with `fetchall_result`).
    """

    def __init__(self, fetchall_result=None, execute_returns=1, routes_result=None):
        self.fetchall_result = fetchall_result or []
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


def test_fetch_eligible_rows_returns_cursor_rows():
    cursor = FakeCursor(fetchall_result=[{"id": 1, "post_type": "someone_take_over", "task_type": "code-analysis-report", "finding": "x"}])
    conn = FakeConnection(cursor)

    rows = fetch_eligible_rows(conn)

    assert rows == [{"id": 1, "post_type": "someone_take_over", "task_type": "code-analysis-report", "finding": "x"}]
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

    won = claim_row(conn, row_id=5)

    assert won is False


def test_mark_dispatched_sets_resolved_when_not_periodic():
    cursor = FakeCursor()
    conn = FakeConnection(cursor)

    mark_dispatched(conn, row_id=7, periodic=False)

    query, params = cursor.executed[0]
    assert "state='resolved'" in query
    assert params == (7,)
    assert conn.committed is True


def test_mark_dispatched_sets_waiting_when_periodic():
    cursor = FakeCursor()
    conn = FakeConnection(cursor)

    mark_dispatched(conn, row_id=7, periodic=True)

    query, params = cursor.executed[0]
    assert "waiting_for_next_periodic_run" in query
    assert params[1] == 7
    assert conn.committed is True


def test_build_prompt_uses_literal_prompt_for_run_me_rows():
    row = {"post_type": "run_me", "prompt": "Clone <repo>, analyse it.", "task_type": None}

    assert build_prompt(row, {}) == "Clone <repo>, analyse it."


def test_build_prompt_routes_someone_take_over_rows_via_routing_table():
    row = {"post_type": "someone_take_over", "task_type": "code-analysis-report", "id": 1, "finding": "some finding"}

    prompt = build_prompt(row, {"code-analysis-report": "report:\n\n$finding"})

    assert "some finding" in prompt


def test_build_prompt_returns_none_for_unrouted_someone_take_over_row():
    row = {"post_type": "someone_take_over", "task_type": "no-such-route", "id": 2, "finding": "n/a"}

    assert build_prompt(row, {"code-analysis-report": "report:\n\n$finding"}) is None


def test_orchestrator_triggers_follow_up_for_known_someone_take_over_row():
    rows = [{"id": 1, "post_type": "someone_take_over", "task_type": "code-analysis-report", "finding": "some finding"}]
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
    queries = [q for q, _ in cursor.executed[1:]]
    assert any("state='dispatching_run'" in q for q in queries)
    assert any("state='resolved'" in q for q in queries)
    assert conn.closed is True


def test_orchestrator_skips_unknown_task_type():
    rows = [{"id": 2, "post_type": "someone_take_over", "task_type": "no-such-route", "finding": "n/a"}]
    cursor = FakeCursor(fetchall_result=rows)
    conn = FakeConnection(cursor)

    with patch("flow.orchestrator_flow._connect", return_value=conn), \
         patch("flow.orchestrator_flow.run_deployment") as mock_run_deployment:
        blackboard_orchestrator.fn()

    mock_run_deployment.assert_not_called()
    # only the two SELECTs ran (eligible rows + routing_rules) -- no
    # claim/dispatch UPDATEs for the unrouted row
    assert len(cursor.executed) == 2


def test_orchestrator_skips_row_lost_to_another_claim():
    rows = [{"id": 3, "post_type": "someone_take_over", "task_type": "code-analysis-report", "finding": "x"}]
    cursor = FakeCursor(fetchall_result=rows, execute_returns=0)  # claim UPDATE affects 0 rows
    conn = FakeConnection(cursor)

    with patch("flow.orchestrator_flow._connect", return_value=conn), \
         patch("flow.orchestrator_flow.run_deployment") as mock_run_deployment:
        blackboard_orchestrator.fn()

    mock_run_deployment.assert_not_called()


def test_orchestrator_triggers_run_me_once_row_and_marks_resolved():
    rows = [{
        "id": 4, "post_type": "run_me", "task_type": None, "prompt": "Clone <repo>, do X.",
        "periodic_interval_minutes": None,
    }]
    cursor = FakeCursor(fetchall_result=rows, execute_returns=1)
    conn = FakeConnection(cursor)

    with patch("flow.orchestrator_flow._connect", return_value=conn), \
         patch("flow.orchestrator_flow.run_deployment") as mock_run_deployment:
        blackboard_orchestrator.fn()

    _, kwargs = mock_run_deployment.call_args
    assert kwargs["parameters"]["prompt"] == "Clone <repo>, do X."
    queries = [q for q, _ in cursor.executed]
    assert any("state='resolved'" in q for q in queries)
    # the dispatch UPDATE must not park it as periodic (the eligibility
    # clause also mentions that state, hence the no-spaces match on the SET)
    assert not any("SET state='waiting_for_next_periodic_run'" in q for q in queries)


def test_orchestrator_triggers_run_me_periodic_row_and_marks_waiting():
    rows = [{
        "id": 5, "post_type": "run_me", "task_type": None, "prompt": "Clone <repo>, do Y.",
        "periodic_interval_minutes": 60,
    }]
    cursor = FakeCursor(fetchall_result=rows, execute_returns=1)
    conn = FakeConnection(cursor)

    with patch("flow.orchestrator_flow._connect", return_value=conn), \
         patch("flow.orchestrator_flow.run_deployment") as mock_run_deployment:
        blackboard_orchestrator.fn()

    _, kwargs = mock_run_deployment.call_args
    assert kwargs["parameters"]["prompt"] == "Clone <repo>, do Y."
    queries = [q for q, _ in cursor.executed[1:]]
    assert any("waiting_for_next_periodic_run" in q for q in queries)
