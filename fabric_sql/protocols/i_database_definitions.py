from typing import Protocol


class IDatabaseDefinitions(Protocol):
    async def get_definitions(self) -> str:
        """Get the database object definition as a formatted string."""
        ...
