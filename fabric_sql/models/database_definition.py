from pydantic import BaseModel

from fabric_sql.models.column_definition import ColumnDefinition
from fabric_sql.models.table_definition import TableDefinition
from fabric_sql.models.view_definition import ViewDefinition


class DatabaseDefinition(BaseModel):
    version: str
    tables: list[TableDefinition]
    views: list[ViewDefinition]
    columns: list[ColumnDefinition]
