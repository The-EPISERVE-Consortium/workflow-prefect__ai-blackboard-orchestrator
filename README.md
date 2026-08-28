# workflow-prefect__generate-ai-task-from-blackboard

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
  follow-up to anything — the equivalent of what used to be a named,
  scheduled deployment in `run-ai-task`, now just a row in this table.
  Fires exactly once if `periodic_interval_minutes` is unset, or recurs
  every `periodic_interval_minutes` if it's set.

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
| `state` | `waiting` → `dispatching_run` → `resolved` (or → `waiting_for_next_periodic_run` → `dispatching_run` → ... for periodic rows) |
| `finding` | The publishing run's output — `post_type='someone_take_over'` rows only |
| `trace` | The publishing run's `trace.html`, attached after the fact — `post_type='someone_take_over'` rows only |
| `periodic_last_triggered_at` | When a periodic row last fired — drives its next eligibility |
| `last_state_change` | Auto-maintained (`ON UPDATE current_timestamp()`) — when `state` last moved |
| `created_at` | Auto |

`state` values, and what they mean regardless of `post_type`:
- **`waiting`** — never dispatched.
- **`dispatching_run`** — claimed by one orchestrator run, which is building
  its prompt and calling `run_deployment` right now (transient, race-guard
  state; says nothing about the triggered run itself — that's a separate,
  unobserved Prefect flow run once dispatched).
- **`waiting_for_next_periodic_run`** — periodic `run_me` rows only: fired
  before, cooling down until due again.
- **`resolved`** — permanently finished: every `someone_take_over` row, and
  every non-recurring `run_me` row, after their single dispatch. A periodic
  row only reaches `resolved` if something deliberately puts it there (e.g.
  to retire it) — it's never terminal by default for that kind.

`resolved` means "handed off to a new run", not "the follow-up run
succeeded" — there's no feedback path back to the row it came from. If the
triggered run itself needs to report something, it publishes its own new
`post_type='someone_take_over'` row via `run-ai-task`'s
`blackboard-communication` skill.

## Flow

**File:** `flow/orchestrator_flow.py`
**Deployment:** `blackboard-orchestrator` (scheduled, see `deploy.py`)

Each run: fetches every row currently eligible (`state='waiting'`, or
`state='waiting_for_next_periodic_run'` whose interval has elapsed), and
for each one: builds its prompt (`build_prompt`), atomically claims it
(`UPDATE ... WHERE <same eligibility clause>`, so two overlapping runs can't
double-dispatch it), calls `run_deployment("agent-task-pipeline/manual",
...)`, then marks it `resolved` or `waiting_for_next_periodic_run` depending
on whether it's a periodic row. Fire-and-forget (`timeout=0`): this flow
doesn't wait for the triggered run to finish.

## Project structure

```
flow/
  orchestrator_flow.py   # the blackboard_orchestrator flow
routing.py                # reads routing_rules; fills a rule's prompt_template ($finding/$prompt/$id/$topic)
migrations/               # hand-run SQL (0001 creates routing_rules, 0002 renames task_type->topic); no runner
tests/                     # pytest unit tests
deploy.py                  # creates/updates the scheduled deployment
Dockerfile                 # python:3.12-slim image for the Prefect worker
.github/workflows/         # test -> build -> push pipeline
```

## `agent_blackboard.routing_rules`

The `topic -> follow-up-prompt` map, replacing what used to be a
hard-coded dict in `routing.py`. Created by `migrations/0001_routing_rules.sql`
(run once as the MariaDB root user — the scoped `blackboard` user can't
`CREATE`).

| Column | Meaning |
|---|---|
| `id` | Primary key |
| `topic` | Unique; matched against a `post_type='someone_take_over'` row's `topic` |
| `prompt_template` | The follow-up prompt. `string.Template` placeholders `$id`, `$topic`, `$prompt`, `$finding` are substituted from the row (`safe_substitute` — unknown `$name` and literal braces pass through); NULL columns become `''`. No other logic — no section slicing, no conditionals |
| `enabled` | `0` disables the rule without deleting it (the scoped user has no `DELETE`); a `topic` with only a disabled rule routes to nothing |
| `created_at` / `updated_at` | Auto (`updated_at` is `ON UPDATE current_timestamp()`) |

Edit rules with SQL, or on `episerve_api_server`'s **AI Blackboard** page
(a Routing Rules section below the task table).

## Local development

```bash
pip install -r requirements.txt -r requirements-test.txt
pytest tests/ -v
```

`.env.example` lists the environment this flow needs at runtime
(`MARIADB_HOST`/`BLACKBOARD_DB`/`BLACKBOARD_USER`/`BLACKBOARD_PASSWORD` to
reach the blackboard, `PREFECT_API_URL` to trigger the follow-up
deployment). In the cluster these come from the `kubernetes-pool` work
pool's base job template (the same place `run-ai-task`'s `ZIB_API_KEY`/
`DISCORD_WEBHOOK_URL` are wired in) — nothing to configure per-deployment.

## Deploy

```bash
PREFECT_API_URL=https://prefect.episerve.zib.de/api python deploy.py
```

Flow code is fetched fresh from git on every run, so only deployment-level
settings (schedule, work pool, image) need a redeploy here — flow logic
changes don't. Polling interval is set via `CRON_SCHEDULE` (default hourly,
`0 * * * *`, `Europe/Berlin`) — not latency-sensitive, since triggering a
follow-up run is asynchronous either way.
