from contextlib import asynccontextmanager
from unittest import mock

import pytest
import pytest_asyncio
from pytest_mock import MockerFixture

from fabric_sql.services.postgres_db_service import PostgresDBService


@pytest_asyncio.fixture
async def mock_service(mocker: MockerFixture) -> PostgresDBService:
    mock_pool = mock.AsyncMock()

    # Mock create_pool to be a coroutine that returns the mock_pool
    async def mock_create_pool(**kwargs):
        return mock_pool

    mocker.patch(
        "fabric_sql.services.postgres_db_service.asyncpg.create_pool",
        side_effect=mock_create_pool,
    )
    mocker.patch(
        "fabric_sql.services.postgres_db_service.PostgresDBService.get_env",
    )

    # Create an instance of PostgresDBService
    db_service = PostgresDBService()
    await db_service._ensure_pool()
    return db_service


@pytest.mark.asyncio
async def test_ensure_pool(mock_service: PostgresDBService):
    await mock_service._ensure_pool()
    assert mock_service._pool is not None


@pytest.mark.asyncio
async def test_close_pool(mock_service: PostgresDBService):
    await mock_service.close()
    # Pool should be None after closing
    assert mock_service._pool is None


@pytest.mark.asyncio
async def test_postgres_db_service_default_cred(mocker: MockerFixture):
    mock_pool = mock.AsyncMock()
    mock_credential = mock.MagicMock()
    mock_credential.get_token.return_value.token = "mock_token"

    # Mock create_pool to be a coroutine that returns the mock_pool
    async def mock_create_pool(**kwargs):
        return mock_pool

    mocker.patch(
        "fabric_sql.services.postgres_db_service.asyncpg.create_pool",
        side_effect=mock_create_pool,
    )
    mocker.patch(
        "fabric_sql.services.postgres_db_service.DefaultAzureCredential",
        return_value=mock_credential,
    )

    mock_env = mock.MagicMock()
    mock_env.postgres_password = None
    mock_env.postgres_host = "localhost"
    mock_env.postgres_port = 5432
    mock_env.postgres_database = "test"
    mock_env.postgres_username = "user"

    mocker.patch(
        "fabric_sql.services.postgres_db_service.PostgresDBService.get_env",
        return_value=mock_env,
    )

    # Create an instance of PostgresDBService
    db_service = PostgresDBService()
    await db_service._ensure_pool()
    assert db_service._pool is not None


@pytest.mark.asyncio
async def test_query(mock_service: PostgresDBService, mocker: MockerFixture):
    # Mock the rows returned by asyncpg - simulating asyncpg.Record objects
    mock_row_1 = {"column1": "a", "column2": "b"}
    mock_row_2 = {"column1": "c", "column2": "d"}
    mock_rows = [mock_row_1, mock_row_2]

    mock_conn = mock.AsyncMock()
    mock_conn.fetch = mock.AsyncMock(return_value=mock_rows)

    # Create a proper async context manager using asynccontextmanager
    @asynccontextmanager
    async def mock_acquire():
        yield mock_conn

    # Mock the pool properly
    mock_service._pool = mock.AsyncMock()
    mock_service._pool.acquire = mock_acquire

    # Define a sample query
    query = "SELECT * FROM test_table"

    # Call the query method
    result = await mock_service.query(query)

    # Assert that the connection fetch was called with the correct query
    mock_conn.fetch.assert_called_once_with(query)
    assert result is not None
    assert len(result) == 2
    assert result[0] == {"column1": "a", "column2": "b"}


@pytest.mark.asyncio
async def test_query_error(mock_service: PostgresDBService, mocker: MockerFixture):
    mock_conn = mock.AsyncMock()
    mock_conn.fetch.side_effect = Exception("Query execution failed")

    # Create a proper async context manager using asynccontextmanager
    @asynccontextmanager
    async def mock_acquire():
        yield mock_conn

    # Mock the pool properly
    mock_service._pool = mock.AsyncMock()
    mock_service._pool.acquire = mock_acquire

    # Define a sample query
    query = "SELECT * FROM test_table"

    result = await mock_service.query(query)
    assert result is None


@pytest.mark.asyncio
async def test_execute(mock_service: PostgresDBService, mocker: MockerFixture):
    mock_conn = mock.AsyncMock()

    # Create a proper async context manager using asynccontextmanager
    @asynccontextmanager
    async def mock_acquire():
        yield mock_conn

    # Mock the pool properly
    mock_service._pool = mock.AsyncMock()
    mock_service._pool.acquire = mock_acquire

    # Define a sample command
    command = "INSERT INTO test_table (id, name) VALUES (1, 'test')"

    # Call the execute method
    await mock_service.execute(command)

    # Assert that the connection execute was called with the correct command
    mock_conn.execute.assert_called_once_with(command)


@pytest.mark.asyncio
async def test_execute_error(mock_service: PostgresDBService, mocker: MockerFixture):
    mock_conn = mock.AsyncMock()
    mock_conn.execute.side_effect = Exception("Execute failed")

    # Create a proper async context manager using asynccontextmanager
    @asynccontextmanager
    async def mock_acquire():
        yield mock_conn

    # Mock the pool properly
    mock_service._pool = mock.AsyncMock()
    mock_service._pool.acquire = mock_acquire

    # Define a sample command
    command = "INSERT INTO test_table (id, name) VALUES (1, 'test')"

    # Should not raise an exception, just print the error
    await mock_service.execute(command)


@pytest.mark.asyncio
async def test_async_context_manager(mocker: MockerFixture):
    """Test async context manager entry and exit methods."""
    mock_pool = mock.AsyncMock()

    # Mock create_pool to be a coroutine that returns the mock_pool
    async def mock_create_pool(**kwargs):
        return mock_pool

    mocker.patch(
        "fabric_sql.services.postgres_db_service.asyncpg.create_pool",
        side_effect=mock_create_pool,
    )
    mocker.patch(
        "fabric_sql.services.postgres_db_service.PostgresDBService.get_env",
    )

    # Test async context manager
    async with PostgresDBService() as db_service:
        assert db_service._pool is not None

    # After exiting context manager, pool should be closed
    assert db_service._pool is None


@pytest.mark.asyncio
async def test_ensure_pool_already_exists(mock_service: PostgresDBService):
    """Test _ensure_pool when pool already exists (line 33 coverage)."""
    # Pool is already created in mock_service fixture
    existing_pool = mock_service._pool

    # Call _ensure_pool again
    await mock_service._ensure_pool()

    # Should still be the same pool (no new pool created)
    assert mock_service._pool is existing_pool


@pytest.mark.asyncio
async def test_query_no_pool(mocker: MockerFixture):
    """Test query method when pool is None (line 68 coverage)."""
    db_service = PostgresDBService()

    # Mock get_env but don't create pool
    mocker.patch(
        "fabric_sql.services.postgres_db_service.PostgresDBService.get_env",
    )

    # Mock _ensure_pool to not actually create a pool
    async def mock_ensure_pool():
        pass  # Don't set self._pool

    mocker.patch.object(db_service, "_ensure_pool", side_effect=mock_ensure_pool)

    result = await db_service.query("SELECT 1")
    assert result is None


@pytest.mark.asyncio
async def test_execute_no_pool(mocker: MockerFixture):
    """Test execute method when pool is None (line 85 coverage)."""
    db_service = PostgresDBService()

    # Mock get_env but don't create pool
    mocker.patch(
        "fabric_sql.services.postgres_db_service.PostgresDBService.get_env",
    )

    # Mock _ensure_pool to not actually create a pool
    async def mock_ensure_pool():
        pass  # Don't set self._pool

    mocker.patch.object(db_service, "_ensure_pool", side_effect=mock_ensure_pool)

    # Should return None without error
    result = await db_service.execute("INSERT INTO test VALUES (1)")
    assert result is None
