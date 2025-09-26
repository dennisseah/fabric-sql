from dataclasses import dataclass

from lagom.environment import Env

from fabric_sql.protocols.i_source_database import ISourceDatabase
from fabric_sql.services.postgres_db_service import DatabaseEnv, PostgresDBService


class SourceDatabaseEnv(Env):
    src_postgres_host: str
    src_postgres_port: int
    src_postgres_database: str
    src_postgres_password: str | None = None
    src_postgres_username: str


@dataclass
class SourceDatabase(PostgresDBService, ISourceDatabase):
    src_env: SourceDatabaseEnv

    def get_env(self) -> DatabaseEnv:
        return DatabaseEnv(
            postgres_host=self.src_env.src_postgres_host,
            postgres_port=self.src_env.src_postgres_port,
            postgres_database=self.src_env.src_postgres_database,
            postgres_password=self.src_env.src_postgres_password,
            postgres_username=self.src_env.src_postgres_username,
        )
