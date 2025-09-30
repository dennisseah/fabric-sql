from typing import Protocol

from fabric_sql.models.table_definition import TableDefinition
from fabric_sql.models.view_definition import ViewDefinition


class IDatabaseDefinitions(Protocol):
    def get_view_definitions(self) -> list[ViewDefinition]:
        """Get the list of view definitions.

        Returns:
            list[ViewDefinition]: List of view definitions.
        """
        ...

    def get_table_definitions(self) -> list[TableDefinition]:
        """Get the list of table definitions.

        Returns:
            list[TableDefinition]: List of table definitions.
        """
        ...

    async def get_definitions(self) -> str:
        """Get the list of view definitions.

        Returns:
            list[ViewDefinition]: List of view definitions.
        """
        ...
