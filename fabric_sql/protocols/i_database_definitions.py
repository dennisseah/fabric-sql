from typing import Protocol

from fabric_sql.models.table_definition import TableDefinition
from fabric_sql.models.view_definition import ViewDefinition


class IDatabaseDefinitions(Protocol):
    def get_table_definitions(self) -> list[TableDefinition]:
        """Get the list of table definitions."""
        ...

    def get_view_definitions(self) -> list[ViewDefinition]:
        """Get the list of view definitions."""
        ...

    async def get_definitions(self) -> str:
        """Get the database object definition as a formatted string."""
        ...
