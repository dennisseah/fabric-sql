from pydantic import BaseModel


class ColumnDefinition(BaseModel):
    name: str
    description: str

    def str_definition(self) -> dict[str, str]:
        return self.model_dump()
