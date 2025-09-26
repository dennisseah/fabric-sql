from pytest_mock import MockerFixture

from fabric_sql.services.source_database import SourceDatabase, SourceDatabaseEnv


def test_source_database_get_env(mocker: MockerFixture):
    mock_env = SourceDatabaseEnv(
        src_postgres_host="fairweather-dev.postgres.database.azure.com",
        src_postgres_port=5432,
        src_postgres_database="test_db",
        src_postgres_password="password",
        src_postgres_username="user",
    )

    mocker.patch("fabric_sql.services.source_database.SourceDatabase.__post_init__")

    svc = SourceDatabase(src_env=mock_env)

    env = svc.get_env()
    assert env.postgres_host == mock_env.src_postgres_host
    assert env.postgres_port == mock_env.src_postgres_port
    assert env.postgres_database == mock_env.src_postgres_database
    assert env.postgres_password == mock_env.src_postgres_password
    assert env.postgres_username == mock_env.src_postgres_username
