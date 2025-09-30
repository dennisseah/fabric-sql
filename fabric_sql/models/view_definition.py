from pydantic import BaseModel


class ViewDefinition(BaseModel):
    db_schema: str = "public"
    name: str
    description: str
    sql: str

    def str_definition(self) -> dict[str, str]:
        dump = self.model_dump()
        del dump["sql"]
        dump["name"] = f"{self.db_schema}.{self.name}"
        del dump["db_schema"]
        return dump
