from routing import load_routes, render_prompt


class _FakeCursor:
    """Minimal stand-in for a pymysql DictCursor used as a context manager."""

    def __init__(self, rows):
        self._rows = rows
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rows):
        self._cursor = _FakeCursor(rows)

    def cursor(self):
        return self._cursor


def test_load_routes_maps_task_type_to_template():
    conn = _FakeConn([
        {"task_type": "code-analysis-report", "prompt_template": "fix $finding"},
        {"task_type": "fix-summary", "prompt_template": "review $finding"},
    ])

    assert load_routes(conn) == {
        "code-analysis-report": "fix $finding",
        "fix-summary": "review $finding",
    }


def test_load_routes_query_filters_on_enabled():
    conn = _FakeConn([])

    load_routes(conn)

    query = conn._cursor.executed[0][0]
    assert "routing_rules" in query
    assert "enabled" in query


def test_render_prompt_substitutes_every_placeholder():
    row = {
        "id": 42,
        "task_type": "code-analysis-report",
        "prompt": "Clone https://example/repo and analyse it.",
        "finding": "X is broken",
    }

    out = render_prompt("report $id ($task_type) from '$prompt':\n$finding", row)

    assert out == (
        "report 42 (code-analysis-report) from "
        "'Clone https://example/repo and analyse it.':\nX is broken"
    )


def test_render_prompt_treats_null_columns_as_empty_string():
    row = {"id": 7, "task_type": None, "prompt": None, "finding": None}

    assert render_prompt("[$prompt][$finding][$task_type]", row) == "[][][]"


def test_render_prompt_passes_through_unknown_placeholders_and_braces():
    row = {"id": 1, "task_type": "t", "prompt": "p", "finding": "f"}

    tmpl = 'keep $unknown and {"json": true} and $$literal'

    assert render_prompt(tmpl, row) == 'keep $unknown and {"json": true} and $literal'


def test_render_prompt_does_not_rescan_substituted_values():
    # A finding that itself contains a `$placeholder` must land verbatim,
    # not trigger a second substitution pass.
    row = {"id": 1, "task_type": "t", "prompt": "p", "finding": "cost is $finding-shaped"}

    assert render_prompt("$finding", row) == "cost is $finding-shaped"
