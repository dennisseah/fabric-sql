import asyncio

from fabric_sql.hosting import container
from fabric_sql.protocols.i_database_definitions import IDatabaseDefinitions
from fabric_sql.protocols.i_duplicate_db_service import (
    DuplicateDBServiceConfig,
    IDuplicateDBService,
)
from fabric_sql.protocols.i_source_database import ISourceDatabase
from fabric_sql.protocols.i_target_database import ITargetDatabase

db_source = container[ISourceDatabase]
db_target = container[ITargetDatabase]
dup_service = container[IDuplicateDBService]
db_definition = container[IDatabaseDefinitions]


def get_tbl_config() -> list[DuplicateDBServiceConfig]:
    return [
        DuplicateDBServiceConfig(
            db_schema=tbl.db_schema, tbl_view=tbl.name, is_view=tbl.is_view
        )
        for tbl in db_definition.get_table_definitions()
    ]


async def create_views_from_sql_file() -> None:
    async with db_target:
        for view in db_definition.get_view_definitions():
            await db_target.execute(
                f"CREATE OR REPLACE VIEW {view.name} AS ({view.sql});"
            )
            print(f"Successfully created {view.name} view")


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
        config=get_tbl_config(),
    )

    await create_views_from_sql_file()


if __name__ == "__main__":
    asyncio.run(main())
