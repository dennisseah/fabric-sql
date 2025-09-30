from pydantic import BaseModel


class TableDefinition(BaseModel):
    db_schema: str = "public"
    name: str
    is_view: bool = False
