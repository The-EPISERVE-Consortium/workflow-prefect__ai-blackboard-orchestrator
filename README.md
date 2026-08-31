# workflow-prefect__ai-blackboard-orchestrator

Prefect flow that polls a shared "blackboard" table for rows describing work
to hand off, and triggers a
[`workflow-prefect__run-ai-task`](https://github.com/The-EPISERVE-Consortium/workflow-prefect__run-ai-task)
run for each one it's eligible to act on.

## The pattern

Each `run-ai-task` run is a fully independent, one-shot container: one
prompt in, whatever it produces out, no shared memory between runs. This
repo is what decides *when* a new run gets triggered and *what prompt* it
gets, based entirely on rows in a shared MariaDB table,
`agent_blackboard.task_runs`. `run-ai-task` itself has a single, permanently
promptless deployment (`manual`) and has no knowledge that this repo exists
or is watching/feeding it; the two repos are coupled only through the shared
table's schema.

A row's `post_type` decides how its prompt is produced:

- **`post_type='someone_take_over'`** — written by a completed `run-ai-task`
  run via its opt-in `blackboard-communication` skill (only when a prompt
  explicitly asks for it). Carries a `finding` payload; this flow builds a
  follow-up prompt from it by filling the `prompt_template` of the matching
  `agent_blackboard.routing_rules` row, keyed on `topic`. A row whose
  `topic` has no *enabled* rule is left alone rather than guessed at —
  add a row to `routing_rules` (SQL, or the AI Blackboard page in
  `episerve_api_server`) to teach the orchestrator about a new kind of
  chainable result. No code change, no redeploy.
- **`post_type='run_me'`** — seeded directly with its own `prompt`, no
  `finding`. This is how a task gets to run at all without being a
  follow-up to anything — a row in this table, not a named deployment in
  `run-ai-task`. Fires exactly once if `periodic_interval_minutes` is
  unset, or recurs every `periodic_interval_minutes` if it's set.

Once this flow has a prompt for a row — built either way — the action is
identical: trigger `run-ai-task`'s `manual` deployment with it.

```
post_type='run_me' row inserted directly (a one-off or recurring seed task)
    │
    ▼                                          run-ai-task (a `run-ai-task` run,
agent_blackboard.task_runs  ◄── polled hourly              prompt asks for blackboard publish)
    │  eligible row claimed, prompt built,                │
    │  run_deployment("agent-task-pipeline/manual", ...)  │  blackboard-communication skill:
    ▼                                                      │  INSERT ... post_type='someone_take_over'
run-ai-task's 'manual' deployment (a fresh one-shot run) ◄─┘
```

## `agent_blackboard.task_runs`

| Column | Meaning |
|---|---|
| `id` | Primary key |
| `topic` | Free-text label; for `post_type='someone_take_over'` rows, matched against `routing_rules.topic` |
| `post_type` | `'run_me'` (seeded directly) or `'someone_take_over'` (a run's output, waiting on a follow-up) |
| `prompt` | For `post_type='run_me'`: the literal prompt to trigger. For `post_type='someone_take_over'`: the prompt that produced `finding` (informational) |
| `periodic_interval_minutes` | Recurrence interval — `post_type='run_me'` rows only; unset means the row fires once |
| `state` | `waiting` → `dispatching_run` → `running` → `resolved` \| `failed` (or `running` → `waiting_for_next_periodic_run` for a periodic `run_me`); `dismissed` is set manually from `episerve_api_server`'s AI Blackboard page to opt a row out |
| `finding` | The publishing run's output — `post_type='someone_take_over'` rows only |
| `trace` | The publishing run's `trace.html`, attached after the fact — `post_type='someone_take_over'` rows only |
| `triggered_flow_run_id` | UUID of the Prefect flow run this row triggered, written by the orchestrator when the row goes to `running` (from `run_deployment`'s returned `FlowRun`). NULL until dispatched, or if none came back. Used by the reconcile pass to read the run's outcome, and by the AI Blackboard page to link to it |
| `periodic_last_triggered_at` | When a periodic row last fired — drives its next eligibility |
| `last_state_change` | Auto-maintained (`ON UPDATE current_timestamp()`) — when `state` last moved |
| `created_at` | Auto |

`state` values, and what they mean regardless of `post_type`:
- **`waiting`** — never dispatched (or re-queued by a human).
- **`dispatching_run`** — claimed by one orchestrator run, which is building
  its prompt and calling `run_deployment` right now (transient race-guard).
- **`running`** — the run has been triggered; the row waits here until a
  later poll's reconcile pass reads the Prefect run's outcome.
- **`waiting_for_next_periodic_run`** — periodic `run_me` rows only: their
  last run finished OK, cooling down until due again (the cooldown is armed
  at *completion*, not at dispatch, so a periodic task can't overlap itself).
- **`resolved`** — the triggered run finished `COMPLETED` and its logs
  contain the agent's `===AGENT_TASKS_COMPLETE===` marker (emitted per the
  `harness-conventions` skill only on real success). Terminal for
  `someone_take_over` rows and non-recurring `run_me` rows.
- **`failed`** — the triggered run ended `FAILED`/`CRASHED`/`CANCELLED`,
  `COMPLETED` without the `===AGENT_TASKS_COMPLETE===` marker (crashed / cut
  off mid-task), or never reached a terminal state within
  `MAX_RUNNING_AGE_MINUTES` (default 360).
  Terminal until a human re-queues it. No failure detail is stored on the
  row — read the Prefect
  run (linked via `triggered_flow_run_id`) and this flow's logs.
- **`dismissed`** — the only state a human sets (never the orchestrator).
  Excluded from `ELIGIBILITY_CLAUSE`. Used to retire a periodic `run_me` row
  or to dismiss a `someone_take_over` row that nothing should route. Set via
  the **Dismiss** action on `episerve_api_server`'s AI Blackboard page; the
  per-row **⋮** menu there also has **Set to Waiting**, which is how a
  `failed` row is retried. The scoped `blackboard` DB user only allows
  `waiting` and `dismissed` to be set by hand.

`resolved` means "the triggered run completed cleanly", not "the work it
did was correct". There is still no feedback path *into* the row's `finding`
— a run that needs to report something publishes its own new
`post_type='someone_take_over'` row via `run-ai-task`'s
`blackboard-communication` skill.

## Flow

**File:** `flow/orchestrator_flow.py`
**Deployment:** `blackboard-orchestrator` (scheduled, see `deploy.py`)

Each run does two passes:

1. **Reconcile.** For every `state='running'` row, read the triggered
   Prefect flow run (`GET /flow_runs/{id}`, raw httpx — no auth in-cluster).
   If it's `COMPLETED`, classify it from its logs (`POST /logs/filter`):
   `clean` if the agent's `AGENT_DONE_MARKER` (`===AGENT_TASKS_COMPLETE===`)
   is present, else `incomplete`. (A log record at `ERROR`+ level or
   containing a traceback is *not* checked — the agent's own tool calls
   routinely hit and recover from lower-level errors while investigating,
   and treating those as a run failure false-positived otherwise-successful
   runs; only Prefect's `COMPLETED` state plus the done marker decide the
   outcome now.) `clean` → `resolved` (or `waiting_for_next_periodic_run`
   with the cooldown armed now, for a periodic `run_me`). `incomplete`,
   `FAILED`/`CRASHED`/`CANCELLED`, or non-terminal older than
   `MAX_RUNNING_AGE_MINUTES` → `failed`. Non-terminal-and-young, or any
   error talking to Prefect, → left `running` for the next poll.
2. **Dispatch.** For every eligible row (`state='waiting'`, or a
   `waiting_for_next_periodic_run` row whose interval has elapsed): build
   its prompt (`build_prompt`), atomically claim it (`UPDATE ... WHERE
   <eligibility clause>`, so two overlapping runs can't double-dispatch),
   `run_deployment("agent-task-pipeline/manual", ..., timeout=0)`, and set
   the row to `running` with the triggered `FlowRun`'s id in
   `triggered_flow_run_id`.

Reconcile runs first so a periodic row whose run just completed can be
re-dispatched in the same poll. `timeout=0` still means this flow never
blocks on a triggered run — it observes the outcome asynchronously, on a
later poll.

Env knobs: `MAX_RUNNING_AGE_MINUTES` (default `360`), `AGENT_DONE_MARKER`
(default `===AGENT_TASKS_COMPLETE===`, kept in sync with the
`harness-conventions` skill in `run-ai-task`). `PREFECT_API_URL` is read for
status/logs, not just by `run_deployment`.

## Project structure

```
flow/
  orchestrator_flow.py   # the blackboard_orchestrator flow
routing.py                # reads routing_rules; fills a rule's prompt_template ($finding/$prompt/$id/$topic)
tests/                     # pytest unit tests
deploy.py                  # creates/updates the scheduled deployment
Dockerfile                 # python:3.12-slim image for the Prefect worker
.github/workflows/         # test -> build -> push pipeline
```

## `agent_blackboard.routing_rules`

The `topic -> follow-up-prompt` map the orchestrator reads to chain a
published finding into a new run. Hand-provisioned on the `agent_blackboard`
database (created as the MariaDB root user — the scoped `blackboard` user
can't `CREATE` — like `task_runs`, there is no schema-as-code for it).

| Column | Meaning |
|---|---|
| `id` | Primary key |
| `topic` | Unique; matched against a `post_type='someone_take_over'` row's `topic` |
| `prompt_template` | The follow-up prompt. `string.Template` placeholders `$id`, `$topic`, `$prompt`, `$finding` are substituted from the row (`safe_substitute` — unknown `$name` and literal braces pass through); NULL columns become `''`. No other logic — no section slicing, no conditionals |
| `enabled` | `0` disables the rule without deleting it (the scoped user has no `DELETE`); a `topic` with only a disabled rule routes to nothing |
| `created_at` / `updated_at` | Auto (`updated_at` is `ON UPDATE current_timestamp()`) |

Edit rules on `episerve_api_server`'s **AI Blackboard** page (a Routing
Rules section below the task table), or with raw SQL. The live table is the
only source of truth — there is no seed file or fixture anywhere in the
repo. Inspect it directly:

```bash
ROOT=$(kubectl get secret mariadb-credentials -n default \
         -o jsonpath='{.data.mariadb-root-password}' | base64 -d)
kubectl exec -n default mariadb-0 -- mariadb -uroot -p"$ROOT" agent_blackboard \
  --raw -e 'SELECT id, topic, enabled, prompt_template FROM routing_rules\G'
```

There is currently one enabled rule, `code-analysis-report`.

## Local development

```bash
pip install -r requirements.txt -r requirements-test.txt
pytest tests/ -v
```

`.env.example` lists the environment this flow needs at runtime
(`MARIADB_HOST`/`BLACKBOARD_DB`/`BLACKBOARD_USER`/`BLACKBOARD_PASSWORD` to
reach the blackboard; `PREFECT_API_URL` to trigger the follow-up deployment
*and* to read triggered runs' state/logs; optional
`MAX_RUNNING_AGE_MINUTES`). In the cluster the required ones come from the
`kubernetes-pool` work pool's base job template (the same place
`run-ai-task`'s `ZIB_API_KEY`/`DISCORD_WEBHOOK_URL` are wired in) — nothing
to configure per-deployment.

## Deploy

```bash
PREFECT_API_URL=https://prefect.episerve.zib.de/api python deploy.py
```

Flow code is fetched fresh from git on every run, so only deployment-level
settings (schedule, work pool, image) need a redeploy here — flow logic
changes don't. Polling interval is set via `CRON_SCHEDULE` (default hourly,
`0 * * * *`, `Europe/Berlin`) — not latency-sensitive, since triggering a
follow-up run is asynchronous either way.
