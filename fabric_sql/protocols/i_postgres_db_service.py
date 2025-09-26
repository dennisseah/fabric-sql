from typing import Any, Protocol, Self


class IPostgresDBService(Protocol):
    async def query(self, query: str) -> list[dict[str, Any]] | None:
        """
        Execute a SQL query against the PostgreSQL database.

        :param query: The SQL query to execute.
        :return: The results of the query.
        """
        ...

    async def execute(self, query: str) -> None:
        """
        Execute a SQL command against the PostgreSQL database.

        :param query: The SQL command to execute.
        """
        ...

    async def __aenter__(self) -> Self:
        """Async context manager entry."""
        ...

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        ...
