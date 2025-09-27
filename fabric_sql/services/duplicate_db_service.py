from fabric_sql.protocols.i_duplicate_db_service import (
    DuplicateDBServiceConfig,
    IDuplicateDBService,
)
from fabric_sql.protocols.i_postgres_db_service import IPostgresDBService


class DuplicateDBService(IDuplicateDBService):
    async def generate_create_table_statement(
        self, db_source: IPostgresDBService, schema_name: str, table_name: str
    ) -> str:
        async with db_source:
            table_def_query = f"""
            SELECT
                'CREATE TABLE ' || schemaname || '.' || tablename || ' (' ||
                string_agg(
                    column_name || ' ' || data_type ||
                    CASE
                        WHEN character_maximum_length IS NOT NULL
                        THEN '(' || character_maximum_length || ')'
                        ELSE ''
                    END ||
                    CASE
                        WHEN is_nullable = 'NO'
                        THEN ' NOT NULL'
                        ELSE ''
                    END,
                    ', '
                ) || ');' as create_statement
            FROM information_schema.columns c
            JOIN pg_tables t ON c.table_name = t.tablename AND c.table_schema = t.schemaname
            WHERE c.table_schema = '{schema_name}' AND c.table_name = '{table_name}'
            GROUP BY schemaname, tablename;
            """  # noqa: E501
            table_definition = await db_source.query(table_def_query)

            if not table_definition:
                raise ValueError(
                    f"Table {schema_name}.{table_name} not found in source database"
                )

            return table_definition[0]["create_statement"]

    async def generate_create_materialized_view_statement(
        self, db_source: IPostgresDBService, schema_name: str, view_name: str
    ) -> str:
        """Generate CREATE MATERIALIZED VIEW statement from source database."""
        async with db_source:
            # Query to get the materialized view definition
            matview_def_query = f"""
            SELECT
                'CREATE MATERIALIZED VIEW ' || schemaname || '.' || matviewname ||
                ' AS ' || definition || ';' as create_statement
            FROM pg_matviews
            WHERE schemaname = '{schema_name}' AND matviewname = '{view_name}';
            """

            matview_definition = await db_source.query(matview_def_query)

            if not matview_definition:
                raise ValueError(
                    f"Materialized view {schema_name}.{view_name} "
                    f"not found in source database"
                )

            return matview_definition[0]["create_statement"]

    async def generate_create_table_from_materialized_view_statement(
        self, db_source: IPostgresDBService, schema_name: str, view_name: str
    ) -> str:
        """Generate CREATE TABLE statement from materialized view structure."""
        async with db_source:
            # First, get the materialized view definition
            matview_query = f"""
            SELECT definition
            FROM pg_matviews
            WHERE schemaname = '{schema_name}' AND matviewname = '{view_name}';
            """

            matview_result = await db_source.query(matview_query)

            if not matview_result:
                raise ValueError(
                    f"Materialized view {schema_name}.{view_name} "
                    f"not found in source database"
                )

            # Create a temporary view to analyze the structure
            temp_view_name = f"temp_analysis_{view_name}"
            definition = matview_result[0]["definition"]

            try:
                # Create temporary view
                temp_view_sql = f"""
                CREATE OR REPLACE VIEW {schema_name}.{temp_view_name} AS {definition};
                """
                await db_source.execute(temp_view_sql)

                # Query the column structure from the temporary view
                table_def_query = f"""
                SELECT
                    'CREATE TABLE ' || '{schema_name}' || '.' || '{view_name}' ||
                    ' (' ||
                    string_agg(
                        column_name || ' ' || data_type ||
                        CASE
                            WHEN character_maximum_length IS NOT NULL
                            THEN '(' || character_maximum_length || ')'
                            ELSE ''
                        END ||
                        CASE
                            WHEN is_nullable = 'NO'
                            THEN ' NOT NULL'
                            ELSE ''
                        END,
                        ', '
                    ) || ');' as create_statement
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}' AND table_name = '{temp_view_name}'
                GROUP BY table_schema, table_name;
                """

                table_definition = await db_source.query(table_def_query)

                if not table_definition:
                    raise ValueError(
                        f"Could not analyze structure of materialized view "
                        f"{schema_name}.{view_name}"
                    )

                return table_definition[0]["create_statement"]

            finally:
                # Clean up temporary view
                cleanup_sql = f"DROP VIEW IF EXISTS {schema_name}.{temp_view_name};"
                await db_source.execute(cleanup_sql)

    async def create_materialized_view(
        self,
        db_target: IPostgresDBService,
        schema_name: str,
        view_name: str,
        create_statement: str,
    ) -> None:
        """Create materialized view in target database."""
        async with db_target:
            drop_sql = f"DROP MATERIALIZED VIEW IF EXISTS {schema_name}.{view_name};"
            await db_target.execute(drop_sql)
            await db_target.execute(create_statement)

    async def refresh_materialized_view(
        self,
        db_target: IPostgresDBService,
        schema_name: str,
        view_name: str,
    ) -> None:
        """Refresh materialized view to populate it with data."""
        async with db_target:
            refresh_sql = f"REFRESH MATERIALIZED VIEW {schema_name}.{view_name};"
            await db_target.execute(refresh_sql)

    async def copy_materialized_view(
        self,
        db_source: IPostgresDBService,
        db_target: IPostgresDBService,
        schema_name: str,
        view_name: str,
    ) -> None:
        """Copy materialized view from source to target database."""
        # Get the materialized view definition
        create_statement = await self.generate_create_materialized_view_statement(
            db_source, schema_name, view_name
        )

        # Create the materialized view in target
        await self.create_materialized_view(
            db_target, schema_name, view_name, create_statement
        )

        # Refresh to populate with data
        await self.refresh_materialized_view(db_target, schema_name, view_name)

        print(f"Successfully copied materialized view {schema_name}.{view_name}")

    async def create_table(
        self,
        db_target: IPostgresDBService,
        schema_name: str,
        table_name: str,
        create_statement: str,
    ) -> None:
        async with db_target:
            await db_target.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name};")
            await db_target.execute(create_statement)

    async def copy_table_data(
        self,
        db_source: IPostgresDBService,
        db_target: IPostgresDBService,
        schema_name: str,
        table_name: str,
    ) -> None:
        async with db_target:
            async with db_source:
                data_result = await db_source.query(
                    f"SELECT * FROM {schema_name}.{table_name};"
                )

                if data_result:
                    columns = list(data_result[0].keys())
                    column_names = ", ".join(columns)

                    for row in data_result:
                        values = []
                        for col in columns:
                            value = row[col]
                            if value is None or value == "None":
                                values.append("NULL")
                            elif isinstance(value, str) and value != "NULL":
                                # Escape single quotes in strings
                                escaped_value = value.replace("'", "''")
                                values.append(f"'{escaped_value}'")
                            else:
                                values.append(str(value))

                        values_str = ", ".join(values)
                        insert_query = (
                            f"INSERT INTO {schema_name}.{table_name} ({column_names}) "
                            f"VALUES ({values_str});"
                        )

                        await db_target.execute(insert_query)

    async def copy_materialized_view_as_table(
        self,
        db_source: IPostgresDBService,
        db_target: IPostgresDBService,
        schema_name: str,
        view_name: str,
    ) -> None:
        """Copy materialized view as a regular table with data."""
        # Get the table structure from materialized view
        create_statement = (
            await self.generate_create_table_from_materialized_view_statement(
                db_source, schema_name, view_name
            )
        )

        # Create the table in target database
        await self.create_table(db_target, schema_name, view_name, create_statement)

        # Copy data from materialized view to table
        await self.copy_table_data(db_source, db_target, schema_name, view_name)

        print(
            f"Successfully copied materialized view {schema_name}.{view_name} as table"
        )

    async def duplicate(
        self,
        source_db: IPostgresDBService,
        target_db: IPostgresDBService,
        config: list[DuplicateDBServiceConfig],
    ) -> None:
        for cfg in config:
            if cfg.is_view:
                # Handle materialized view - create as regular table
                await self.copy_materialized_view_as_table(
                    source_db, target_db, cfg.db_schema, cfg.tbl_view
                )
            else:
                # Handle regular table
                create_statement = await self.generate_create_table_statement(
                    source_db, cfg.db_schema, cfg.tbl_view
                )
                await self.create_table(
                    target_db, cfg.db_schema, cfg.tbl_view, create_statement
                )
                await self.copy_table_data(
                    source_db, target_db, cfg.db_schema, cfg.tbl_view
                )
                print(f"Successfully copied table {cfg.db_schema}.{cfg.tbl_view}")
