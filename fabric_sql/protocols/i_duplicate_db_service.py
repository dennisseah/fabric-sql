from typing import Protocol

from pydantic import BaseModel

from fabric_sql.protocols.i_postgres_db_service import IPostgresDBService


class DuplicateDBServiceConfig(BaseModel):
    db_schema: str
    tbl_view: str
    is_view: bool = False


class IDuplicateDBService(Protocol):
    async def duplicate(
        self,
        source_db: IPostgresDBService,
        target_db: IPostgresDBService,
        config: list[DuplicateDBServiceConfig],
    ) -> None:
        """
        Duplicate a database from source_db to target_db.

        :param source_db: The source database service.
        :param target_db: The target database service.
        :param config: Configuration for tables and views to duplicate.
        """
        ...

    async def copy_materialized_view(
        self,
        db_source: IPostgresDBService,
        db_target: IPostgresDBService,
        schema_name: str,
        view_name: str,
    ) -> None:
        """
        Copy a materialized view from source to target database.

        :param db_source: The source database service.
        :param db_target: The target database service.
        :param schema_name: The schema name.
        :param view_name: The materialized view name.
        """
        ...

    async def copy_materialized_view_as_table(
        self,
        db_source: IPostgresDBService,
        db_target: IPostgresDBService,
        schema_name: str,
        view_name: str,
    ) -> None:
        """
        Copy a materialized view as a regular table in target database.

        :param db_source: The source database service.
        :param db_target: The target database service.
        :param schema_name: The schema name.
        :param view_name: The materialized view name.
        """
        ...
