from pydantic import BaseModel


class TableDefinition(BaseModel):
    db_schema: str
    name: str
    is_view: bool = False
