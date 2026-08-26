"""Register the blackboard_orchestrator flow as a scheduled Prefect deployment.

Run once (or on every release) from a machine that can reach the Prefect
server:

    PREFECT_API_URL=https://prefect.episerve.zib.de/api python deploy.py

Flow code is fetched fresh from git on every run (see GitRepository below),
so only deployment-level settings (schedule, work pool, image) need a
redeploy here -- flow logic changes don't.
"""

import os

from prefect.client.schemas.schedules import CronSchedule
from prefect.runner.storage import GitRepository

from flow.orchestrator_flow import blackboard_orchestrator

GITHUB_REPO_URL = "https://github.com/The-EPISERVE-Consortium/workflow-prefect__generate-ai-task-from-blackboard"
DOCKER_IMAGE = "ghcr.io/the-episerve-consortium/workflow-prefect__generate-ai-task-from-blackboard:main"
WORK_POOL_NAME = os.getenv("WORK_POOL_NAME", "kubernetes-pool")
DEPLOYMENT_NAME = os.getenv("DEPLOYMENT_NAME", "blackboard-orchestrator")

# Polling interval -- how often unclaimed blackboard rows get picked up.
# Not latency-sensitive (this triggers a follow-up one-shot agent run, not a
# synchronous handoff), so a coarse interval is fine; tune via cron env var
# rather than code if it needs to change.
CRON_SCHEDULE = os.getenv("CRON_SCHEDULE", "0 * * * *")

if __name__ == "__main__":
    blackboard_orchestrator.from_source(
        source=GitRepository(url=GITHUB_REPO_URL, branch="main"),
        entrypoint="flow/orchestrator_flow.py:blackboard_orchestrator",
    ).deploy(
        name=DEPLOYMENT_NAME,
        work_pool_name=WORK_POOL_NAME,
        job_variables={"image": DOCKER_IMAGE, "image_pull_policy": "Always"},
        schedules=[CronSchedule(cron=CRON_SCHEDULE, timezone="Europe/Berlin")],
    )
