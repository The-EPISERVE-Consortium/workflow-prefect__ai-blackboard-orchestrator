-- 0001_routing_rules.sql
--
-- Adds the routing table that replaces the hard-coded ROUTES dict that used
-- to live in routing.py: a `task_type -> follow-up-prompt-template` lookup
-- the orchestrator reads at poll time. Changing a chain is now an
-- INSERT/UPDATE here (or an edit on episerve_api_server's AI Blackboard
-- page), with no code change and no redeploy.
--
-- Run once, by hand, as the MariaDB *root* user -- the scoped `blackboard`
-- user has SELECT/INSERT/UPDATE on `agent_blackboard.*` only, no CREATE:
--
--   ROOT=$(kubectl get secret mariadb-credentials -n default \
--            -o jsonpath='{.data.mariadb-root-password}' | base64 -d)
--   mysql -h 130.73.130.213 -u root -p"$ROOT" agent_blackboard \
--            < migrations/0001_routing_rules.sql
--
-- No GRANT is needed afterwards: the `blackboard` user's existing
-- SELECT/INSERT/UPDATE on `agent_blackboard.*` already covers the
-- orchestrator reading this table and episerve_api_server editing it.
-- There is no schema-as-code for this database anywhere in the monorepo --
-- `task_runs` itself is hand-provisioned -- so this file is the record of
-- the change, not something a migration runner applies.

CREATE TABLE IF NOT EXISTS routing_rules (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    task_type       VARCHAR(255) NOT NULL UNIQUE,
    prompt_template LONGTEXT     NOT NULL,
    enabled         TINYINT(1)   NOT NULL DEFAULT 1,
    created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                                 ON UPDATE CURRENT_TIMESTAMP
) CHARACTER SET utf8mb4;

-- Seed: the one route that used to be _code_analysis_report_to_fix_prompt
-- in routing.py. The old Python builder sliced the "## Potential Bug
-- Analysis" section out of the report and branched on whether the
-- originating prompt was recorded. The table-driven version has no logic:
-- the whole `finding` goes through via $finding, and $prompt is always
-- included verbatim.
INSERT INTO routing_rules (task_type, prompt_template) VALUES (
    'code-analysis-report',
    'A code analysis report (blackboard task_runs.id=$id) found the following potential bugs:\n\n$finding\n\nThat report was produced by this task: $prompt\n\nClone the repository referenced above, verify each finding against the actual code, fix the ones that are real bugs, and open a PR with your changes.\n\nAfterwards, produce a brief report to /output/report.pdf of what you fixed, how you fixed it, and your reasoning for each fix, and publish it to the blackboard with task_type=''fix-summary'' and send the PDF to Discord.'
) ON DUPLICATE KEY UPDATE prompt_template = VALUES(prompt_template);
