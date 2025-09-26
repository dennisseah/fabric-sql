import asyncio
import os

from fabric_sql.hosting import container
from fabric_sql.protocols.i_source_database import ISourceDatabase
from fabric_sql.protocols.i_target_database import ITargetDatabase

db_source = container[ISourceDatabase]
db_target = container[ITargetDatabase]

tables = [
    ("public", "config"),
    ("public", "framework_compliance_data"),
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


async def generate_create_table_statement(schema_name: str, table_name: str) -> str:
    async with db_source:
        # Query to get the CREATE TABLE statement for public.config
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
        """

        table_definition = await db_source.query(table_def_query)

        if not table_definition:
            raise ValueError(
                f"Table {schema_name}.{table_name} not found in source database"
            )

        return table_definition[0]["create_statement"]


async def create_table(
    schema_name: str, table_name: str, create_statement: str
) -> None:
    async with db_target:
        await db_target.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name};")
        await db_target.execute(create_statement)


async def copy_table_data(schema_name: str, table_name: str) -> None:
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


async def main():
    # Step 1: Reset the target database schema
    async with db_target:
        try:
            await db_target.execute("DROP SCHEMA IF EXISTS public CASCADE;")
        except Exception:
            pass
        await db_target.execute("CREATE SCHEMA public;")

    # Step 2: Copy all tables from source to target
    for tbl in tables:
        print(f"Processing table {tbl[0]}.{tbl[1]}...")
        create_statement = await generate_create_table_statement(tbl[0], tbl[1])
        await create_table(tbl[0], tbl[1], create_statement)
        await copy_table_data(tbl[0], tbl[1])
        print(f"Successfully copied table {tbl[0]}.{tbl[1]}")

    # Step 3: Create views from SQL file
    await create_views_from_sql_file(os.path.join("scripts", "create_view.sql"))


if __name__ == "__main__":
    asyncio.run(main())
