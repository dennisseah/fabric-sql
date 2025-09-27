from dataclasses import dataclass

import yaml
from tabulate import tabulate

from fabric_sql import DB_DEFINITION_PATH
from fabric_sql.models.table_definition import TableDefinition
from fabric_sql.models.view_definition import ViewDefinition
from fabric_sql.protocols.i_database_definitions import IDatabaseDefinitions
from fabric_sql.protocols.i_target_database import ITargetDatabase


@dataclass
class DatabaseDefinitions(IDatabaseDefinitions):
    target_db: ITargetDatabase
    view_definitions: dict[str, list[dict[str, str]]]

    def __post_init__(self) -> None:
        with open(DB_DEFINITION_PATH, "r") as file:
            self.definitions = yaml.safe_load(file)

    def get_table_definitions(self) -> list[TableDefinition]:
        defns = []
        for tbl in self.definitions["tables"]:
            parts = tbl["name"].split(".")
            defns.append(
                TableDefinition(
                    db_schema=parts[0],
                    name=parts[1],
                    is_view=tbl.get("is_view", False),
                )
            )
        return defns

    def get_view_definitions(self) -> list[ViewDefinition]:
        defns = []
        for view in self.definitions["views"]:
            parts = view["name"].split(".")
            defns.append(
                ViewDefinition(db_schema=parts[0], name=parts[1], sql=view["sql"])
            )
        return defns

    async def get_view_schemas(self) -> dict[str, list[dict[str, str]]]:
        if self.view_definitions:
            return self.view_definitions

        for view in self.definitions.get("views", []):
            parts = view["name"].split(".")
            self.view_definitions[
                view["name"]
            ] = await self.target_db.show_view_definition(parts[0], parts[1])

        return self.view_definitions

    async def get_definitions(self) -> str:
        col_definitions = tabulate(
            self.definitions["columns"], headers="keys", tablefmt="grid"
        )
        view_definitions = tabulate(
            self.definitions["views"], headers="keys", tablefmt="grid"
        )
        buff = []
        view_defn = await self.get_view_schemas()
        for view_name, cols in view_defn.items():
            buff.append(f"View: {view_name}")
            buff.append(tabulate(cols, headers="keys", tablefmt="grid"))
            buff.append("")

        return f"""Column Definitions:
{col_definitions}

Database View Definitions:
{view_definitions}

View SQL Definitions:
{chr(10).join(buff)}"""
