from pydantic import BaseModel


class ViewDefinition(BaseModel):
    db_schema: str
    name: str
    sql: str
