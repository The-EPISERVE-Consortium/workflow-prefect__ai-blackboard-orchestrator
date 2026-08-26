# workflow-prefect__generate-ai-task-from-blackboard

Prefect flow that polls a shared "blackboard" table for results published by
one-shot agent runs from
[`workflow-prefect__run-ai-task`](https://github.com/The-EPISERVE-Consortium/workflow-prefect__run-ai-task),
and triggers a follow-up one-shot run for any it recognizes.

## The pattern

Each `run-ai-task` run is a fully independent, one-shot container: one
prompt in, whatever it produces out, no shared memory between runs. To chain
runs together (e.g. "agent A writes a bug report" → "agent B fixes the bugs
it found") without coupling the two repos directly, `run-ai-task` gained an
opt-in `blackboard-communication` skill: a task whose prompt explicitly asks
for it publishes its result as a row in a shared MariaDB table,
`agent_blackboard.task_runs`.

This repo is the other half — the *read* side. It knows nothing about how a
row got there, only how to interpret its `task_type` column (see
`routing.py`) and what prompt to hand to the next run. `run-ai-task` has no
knowledge that this repo exists or is watching its output; the two are
coupled only through the shared table's schema.

```
run-ai-task (task A, prompt asks for blackboard publish)
    │  INSERT INTO task_runs (task_type, result) VALUES (...)
    ▼
agent_blackboard.task_runs  ◄── this repo polls this table on a schedule
    │  claims row, builds a prompt from routing.py, triggers:
    ▼
run-ai-task's 'manual' deployment (task B, a fresh one-shot run)
```

## Flow

**File:** `flow/orchestrator_flow.py`
**Deployment:** `blackboard-orchestrator` (scheduled, see `deploy.py`)

Each run: `SELECT ... WHERE status='new'`, then for every row whose
`task_type` has an entry in `routing.py`, atomically claims it
(`UPDATE ... WHERE status='new'`, so two overlapping runs can't double up),
builds a prompt, and calls `run_deployment("agent-task-pipeline/manual", ...)`
to trigger a fresh `run-ai-task` run — the same one-off-prompt path
`run-ai-task/run.py` and a human both already use. Fire-and-forget
(`timeout=0`): this flow doesn't wait for the triggered run to finish.

A row whose `task_type` has no route is left as `status='new'` rather than
guessed at — add an entry to `routing.py` to make the orchestrator act on a
new kind of result; no other code needs to change.

`status='done'` means "handed off to a new run", not "the follow-up run
succeeded" — there's no feedback path back to this row. If the follow-up run
itself needs to report something, it publishes its own new row via
`run-ai-task`'s `blackboard-communication` skill.

## Project structure

```
flow/
  orchestrator_flow.py   # the blackboard_orchestrator flow
routing.py                # task_type -> prompt-builder registry
tests/                     # pytest unit tests
deploy.py                  # creates/updates the scheduled deployment
Dockerfile                 # python:3.12-slim image for the Prefect worker
.github/workflows/         # test -> build -> push pipeline
```

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
changes don't. Polling interval is set via `CRON_SCHEDULE` (default every 15
minutes, `Europe/Berlin`) — not latency-sensitive, since triggering a
follow-up run is asynchronous either way.
