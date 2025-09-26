from pytest_mock import MockerFixture

from fabric_sql.services.target_database import TargetDatabase, TargetDatabaseEnv


def test_source_database_get_env(mocker: MockerFixture):
    mock_env = TargetDatabaseEnv(
        dest_postgres_host="fairweather-dev.postgres.database.azure.com",
        dest_postgres_port=5432,
        dest_postgres_database="test_db",
        dest_postgres_password="password",
        dest_postgres_username="user",
    )

    mocker.patch("fabric_sql.services.target_database.TargetDatabase.__post_init__")

    svc = TargetDatabase(src_env=mock_env)

    env = svc.get_env()
    assert env.postgres_host == mock_env.dest_postgres_host
    assert env.postgres_port == mock_env.dest_postgres_port
    assert env.postgres_database == mock_env.dest_postgres_database
    assert env.postgres_password == mock_env.dest_postgres_password
    assert env.postgres_username == mock_env.dest_postgres_username
