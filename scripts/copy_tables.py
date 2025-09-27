import asyncio
import os

from fabric_sql.hosting import container
from fabric_sql.protocols.i_duplicate_db_service import (
    DuplicateDBServiceConfig,
    IDuplicateDBService,
)
from fabric_sql.protocols.i_source_database import ISourceDatabase
from fabric_sql.protocols.i_target_database import ITargetDatabase

db_source = container[ISourceDatabase]
db_target = container[ITargetDatabase]
dup_service = container[IDuplicateDBService]

config = [
    DuplicateDBServiceConfig(db_schema="public", tbl_view="config"),
    DuplicateDBServiceConfig(db_schema="public", tbl_view="framework_compliance_data"),
    DuplicateDBServiceConfig(
        db_schema="public", tbl_view="mv_policy_compliance", is_view=True
    ),
]


async def create_views_from_sql_file(file_path: str) -> None:
    """Read and execute SQL statements from create_view.sql file."""
    try:
        with open(file_path, "r") as file:
            sql_content = file.read()

        statements = []
        current_statement = []

        for line in sql_content.split("\n"):
            line = line.strip()
            if line and not line.startswith("--"):
                current_statement.append(line)
                if line.endswith(";"):
                    statements.append(" ".join(current_statement))
                    current_statement = []

        async with db_target:
            for statement in statements:
                if statement.strip():
                    await db_target.execute(statement)

        print(f"Successfully created {len(statements)} views from {file_path}")
    except FileNotFoundError:
        print(f"SQL file not found: {file_path}")
    except Exception as e:
        print(f"Error executing SQL file: {e}")


async def main():
    async with db_target:
        try:
            await db_target.execute("DROP SCHEMA IF EXISTS public CASCADE;")
        except Exception:
            pass
        await db_target.execute("CREATE SCHEMA public;")

    await dup_service.duplicate(
        source_db=db_source,
        target_db=db_target,
        config=config,
    )

    await create_views_from_sql_file(os.path.join("scripts", "create_view.sql"))


if __name__ == "__main__":
    asyncio.run(main())
