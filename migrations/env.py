from logging.config import fileConfig

from alembic import context

import models  # noqa: F401  (registers the tables on Base.metadata)
from database import Base, engine

if context.config.config_file_name:
    fileConfig(context.config.config_file_name)


def run_migrations() -> None:
    # Tests pass their own connection; otherwise use the app's DATABASE_URL.
    connection = context.config.attributes.get("connection")
    if connection is None:
        with engine.connect() as connection:
            migrate(connection)
    else:
        migrate(connection)


def migrate(connection) -> None:
    # Batch mode lets ALTER TABLE work on SQLite (used by the tests).
    context.configure(
        connection=connection, target_metadata=Base.metadata, render_as_batch=True, compare_type=True
    )
    with context.begin_transaction():
        context.run_migrations()


run_migrations()
