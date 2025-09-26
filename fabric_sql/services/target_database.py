from dataclasses import dataclass

from lagom.environment import Env

from fabric_sql.protocols.i_target_database import ITargetDatabase
from fabric_sql.services.postgres_db_service import DatabaseEnv, PostgresDBService


class TargetDatabaseEnv(Env):
    dest_postgres_host: str
    dest_postgres_port: int
    dest_postgres_database: str
    dest_postgres_password: str | None = None
    dest_postgres_username: str


@dataclass
class TargetDatabase(PostgresDBService, ITargetDatabase):
    src_env: TargetDatabaseEnv

    def get_env(self) -> DatabaseEnv:
        return DatabaseEnv(
            postgres_host=self.src_env.dest_postgres_host,
            postgres_port=self.src_env.dest_postgres_port,
            postgres_database=self.src_env.dest_postgres_database,
            postgres_password=self.src_env.dest_postgres_password,
            postgres_username=self.src_env.dest_postgres_username,
        )
