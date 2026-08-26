import os
import sys

# Set required env vars before flow/ is imported (module-level reads happen
# inside _connect(), but setting these up front keeps every test independent
# of import order).
os.environ.setdefault("MARIADB_HOST", "test-mariadb")
os.environ.setdefault("BLACKBOARD_DB", "test-db")
os.environ.setdefault("BLACKBOARD_USER", "test-user")
os.environ.setdefault("BLACKBOARD_PASSWORD", "test-password")

# Ensure project root is on sys.path so `flow.*`/`routing` imports resolve
# the same way they do when the Prefect worker runs this flow.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
