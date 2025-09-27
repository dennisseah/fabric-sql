from dataclasses import dataclass
from typing import Self

import asyncpg
from azure.identity import DefaultAzureCredential
from pydantic import BaseModel

from fabric_sql.protocols.i_postgres_db_service import IPostgresDBService


class DatabaseEnv(BaseModel):
    postgres_host: str
    postgres_port: int
    postgres_database: str
    postgres_password: str | None = None
    postgres_username: str


@dataclass
class PostgresDBService(IPostgresDBService):
    def get_env(self) -> DatabaseEnv: ...

    def __post_init__(self) -> None:
        self._pool: asyncpg.Pool | None = None

    async def __aenter__(self) -> Self:
        """Async context manager entry."""
        await self._ensure_pool()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()

    async def _ensure_pool(self) -> None:
        """Ensure the connection pool is created."""
        if self._pool is None:
            env = self.get_env()
            password = env.postgres_password

            if not password:
                credential = DefaultAzureCredential()
                password = credential.get_token(
                    "https://ossrdbms-aad.database.windows.net/.default"
                ).token

            self._pool = await asyncpg.create_pool(
                host=env.postgres_host,
                database=env.postgres_database,
                user=env.postgres_username,
                password=password,
                port=env.postgres_port,
                ssl="require",
                min_size=2,
                max_size=10,
            )

    async def close(self) -> None:
        """Close the connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def query(self, query: str) -> list[dict[str, str]] | None:
        """Execute a query and return results as a list of dictionaries."""
        await self._ensure_pool()
        if not self._pool:
            return None

        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(query)
                results = []
                for row in rows:
                    results.append({key: str(value) for key, value in row.items()})
                return results
        except Exception as e:
            print(f"Query failed: {e}")
            return None

    async def execute(self, query: str) -> None:
        """Execute a command (INSERT, UPDATE, DELETE, etc.)."""
        await self._ensure_pool()
        if not self._pool:
            return

        try:
            async with self._pool.acquire() as conn:
                await conn.execute(query)
        except Exception as e:
            print(f"Execute failed: {e}")

    async def show_view_definition(
        self, schema: str, view_name: str
    ) -> list[dict[str, str]]:
        query = f"""SELECT
            column_name,
            data_type,
            CASE
                WHEN character_maximum_length IS NOT NULL
                THEN data_type || '(' || character_maximum_length || ')'
                WHEN numeric_precision IS NOT NULL AND numeric_scale IS NOT NULL
                THEN data_type || '(' || numeric_precision || ',' || numeric_scale || ')'
                WHEN numeric_precision IS NOT NULL
                THEN data_type || '(' || numeric_precision || ')'
                ELSE data_type
            END AS full_data_type,
            is_nullable,
            column_default,
            ordinal_position
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
            AND table_name = '{view_name}'
        ORDER BY ordinal_position;"""  # noqa: E501

        results = await self.query(query)
        return results if results else []
