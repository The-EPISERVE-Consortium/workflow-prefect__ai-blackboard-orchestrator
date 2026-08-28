-- 0002_rename_task_type_to_topic.sql
--
-- Renames `task_type` -> `topic` in both blackboard tables. The column is
-- the label a published `someone_take_over` row is tagged with and the key
-- the orchestrator matches against `routing_rules` to pick a follow-up
-- prompt -- "topic" (blackboard / pub-sub terminology) describes that role
-- better than "task_type".
--
-- Run as the MariaDB *root* user (the scoped `blackboard` user cannot ALTER):
--
--   ROOT=$(kubectl get secret mariadb-credentials -n default \
--            -o jsonpath='{.data.mariadb-root-password}' | base64 -d)
--   mysql -h 130.73.130.213 -u root -p"$ROOT" agent_blackboard \
--            < migrations/0002_rename_task_type_to_topic.sql
--
-- Already applied to the live database. Renaming a column with CHANGE keeps
-- its data and indexes (routing_rules' UNIQUE index moves to `topic`).

ALTER TABLE task_runs      CHANGE COLUMN task_type topic VARCHAR(255) NULL;
ALTER TABLE routing_rules  CHANGE COLUMN task_type topic VARCHAR(255) NOT NULL;

-- Keep the seeded rule's own instruction text consistent with the column:
-- the template tells the follow-up run which value to tag its published row
-- with (`task_type='...'` -> `topic='...'`).
UPDATE routing_rules
   SET prompt_template = REPLACE(prompt_template, 'task_type=''', 'topic=''')
 WHERE prompt_template LIKE '%task_type=''%';
