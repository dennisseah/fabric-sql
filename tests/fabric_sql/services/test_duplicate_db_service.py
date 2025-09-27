from unittest.mock import AsyncMock, MagicMock

import pytest

from fabric_sql.protocols.i_duplicate_db_service import DuplicateDBServiceConfig
from fabric_sql.protocols.i_postgres_db_service import IPostgresDBService
from fabric_sql.services.duplicate_db_service import DuplicateDBService


@pytest.mark.asyncio
async def test_generate_create_table_statement():
    mock_source_db = MagicMock(spec=IPostgresDBService)
    mock_source_db.query = AsyncMock(
        return_value=[
            {"create_statement": "CREATE TABLE public.test (id INT NOT NULL);"}
        ]
    )

    dup_service = DuplicateDBService()
    await dup_service.generate_create_table_statement(mock_source_db, "public", "test")
    mock_source_db.query.assert_awaited_once()


@pytest.mark.asyncio
async def test_generate_create_table_statement_err():
    mock_source_db = MagicMock(spec=IPostgresDBService)
    mock_source_db.query = AsyncMock(return_value=[])

    dup_service = DuplicateDBService()
    with pytest.raises(
        ValueError, match="Table public.test not found in source database"
    ):
        await dup_service.generate_create_table_statement(
            mock_source_db, "public", "test"
        )


@pytest.mark.asyncio
async def test_create_table():
    mock_target_db = MagicMock(spec=IPostgresDBService)
    mock_target_db.execute = AsyncMock()

    dup_service = DuplicateDBService()
    await dup_service.create_table(
        mock_target_db,
        "public",
        "test",
        "CREATE TABLE public.test (id INT NOT NULL);",
    )

    mock_target_db.execute.assert_any_call("DROP TABLE IF EXISTS public.test;")
    mock_target_db.execute.assert_any_call(
        "CREATE TABLE public.test (id INT NOT NULL);"
    )


@pytest.mark.asyncio
async def test_copy_table_data():
    mock_source_db = MagicMock(spec=IPostgresDBService)
    mock_source_db.query = AsyncMock(
        return_value=[
            {"id": 1},
            {"id": "a"},
            {"id": "a'"},
            {"id": None},
        ]
    )

    mock_target_db = MagicMock(spec=IPostgresDBService)
    mock_target_db.execute = AsyncMock()

    dup_service = DuplicateDBService()
    await dup_service.copy_table_data(mock_source_db, mock_target_db, "public", "test")

    mock_source_db.query.assert_awaited_once_with("SELECT * FROM public.test;")
    assert mock_target_db.execute.call_count == 4
    mock_target_db.execute.assert_any_call("INSERT INTO public.test (id) VALUES (1);")
    mock_target_db.execute.assert_any_call("INSERT INTO public.test (id) VALUES ('a');")
    mock_target_db.execute.assert_any_call(
        "INSERT INTO public.test (id) VALUES ('a''');"
    )
    mock_target_db.execute.assert_any_call(
        "INSERT INTO public.test (id) VALUES (NULL);"
    )


@pytest.mark.asyncio
async def test_generate_create_materialized_view_statement():
    """Test successful generation of CREATE MATERIALIZED VIEW statement."""
    mock_source_db = MagicMock(spec=IPostgresDBService)
    expected_create_statement = (
        "CREATE MATERIALIZED VIEW public.user_summary AS "
        "SELECT user_id, COUNT(*) FROM users GROUP BY user_id;"
    )
    mock_source_db.query = AsyncMock(
        return_value=[{"create_statement": expected_create_statement}]
    )

    dup_service = DuplicateDBService()
    result = await dup_service.generate_create_materialized_view_statement(
        mock_source_db, "public", "user_summary"
    )

    # Verify the correct query was called
    mock_source_db.query.assert_awaited_once()

    # Verify the result
    assert result == expected_create_statement


@pytest.mark.asyncio
async def test_generate_create_materialized_view_statement_not_found():
    """Test error handling when materialized view doesn't exist."""
    mock_source_db = MagicMock(spec=IPostgresDBService)
    mock_source_db.query = AsyncMock(return_value=[])  # Empty result

    dup_service = DuplicateDBService()

    with pytest.raises(
        ValueError,
        match="Materialized view public.nonexistent not found in source database",
    ):
        await dup_service.generate_create_materialized_view_statement(
            mock_source_db, "public", "nonexistent"
        )

    mock_source_db.query.assert_awaited_once()


@pytest.mark.asyncio
async def test_generate_create_materialized_view_statement_none_result():
    """Test error handling when query returns None."""
    mock_source_db = MagicMock(spec=IPostgresDBService)
    mock_source_db.query = AsyncMock(return_value=None)

    dup_service = DuplicateDBService()

    with pytest.raises(
        ValueError,
        match="Materialized view analytics.stats not found in source database",
    ):
        await dup_service.generate_create_materialized_view_statement(
            mock_source_db, "analytics", "stats"
        )


@pytest.mark.asyncio
async def test_generate_create_materialized_view_statement_complex():
    """Test with complex materialized view definition."""
    mock_source_db = MagicMock(spec=IPostgresDBService)
    complex_create_statement = (
        "CREATE MATERIALIZED VIEW analytics.daily_stats AS "
        "SELECT date_trunc('day', created_at) AS day, "
        "COUNT(*) AS total_orders, "
        "SUM(amount) AS total_revenue, "
        "AVG(amount) AS avg_order_value "
        "FROM orders WHERE status = 'completed' "
        "GROUP BY date_trunc('day', created_at) "
        "ORDER BY day DESC;"
    )

    mock_source_db.query = AsyncMock(
        return_value=[{"create_statement": complex_create_statement}]
    )

    dup_service = DuplicateDBService()
    result = await dup_service.generate_create_materialized_view_statement(
        mock_source_db, "analytics", "daily_stats"
    )

    assert result == complex_create_statement
    mock_source_db.query.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_materialized_view():
    """Test creating materialized view in target database."""
    mock_target_db = MagicMock(spec=IPostgresDBService)
    mock_target_db.execute = AsyncMock()

    dup_service = DuplicateDBService()
    create_statement = (
        "CREATE MATERIALIZED VIEW public.stats AS SELECT COUNT(*) FROM users;"
    )

    await dup_service.create_materialized_view(
        mock_target_db, "public", "stats", create_statement
    )

    # Verify both DROP and CREATE statements were executed
    mock_target_db.execute.assert_any_call(
        "DROP MATERIALIZED VIEW IF EXISTS public.stats;"
    )
    mock_target_db.execute.assert_any_call(create_statement)
    assert mock_target_db.execute.call_count == 2


@pytest.mark.asyncio
async def test_refresh_materialized_view():
    """Test refreshing materialized view."""
    mock_target_db = MagicMock(spec=IPostgresDBService)
    mock_target_db.execute = AsyncMock()

    dup_service = DuplicateDBService()

    await dup_service.refresh_materialized_view(
        mock_target_db, "analytics", "monthly_report"
    )

    mock_target_db.execute.assert_awaited_once_with(
        "REFRESH MATERIALIZED VIEW analytics.monthly_report;"
    )


@pytest.mark.asyncio
async def test_copy_materialized_view():
    """Test complete materialized view copy process."""
    mock_source_db = MagicMock(spec=IPostgresDBService)
    mock_target_db = MagicMock(spec=IPostgresDBService)

    dup_service = DuplicateDBService()

    # Mock the methods that copy_materialized_view calls
    expected_create_statement = (
        "CREATE MATERIALIZED VIEW public.summary AS SELECT * FROM data;"
    )
    dup_service.generate_create_materialized_view_statement = AsyncMock(
        return_value=expected_create_statement
    )
    dup_service.create_materialized_view = AsyncMock()
    dup_service.refresh_materialized_view = AsyncMock()

    await dup_service.copy_materialized_view(
        mock_source_db, mock_target_db, "public", "summary"
    )

    # Verify all three steps were called
    dup_service.generate_create_materialized_view_statement.assert_awaited_once_with(
        mock_source_db, "public", "summary"
    )
    dup_service.create_materialized_view.assert_awaited_once_with(
        mock_target_db, "public", "summary", expected_create_statement
    )
    dup_service.refresh_materialized_view.assert_awaited_once_with(
        mock_target_db, "public", "summary"
    )


@pytest.mark.asyncio
async def test_duplicate_with_materialized_views():
    """Test duplicate method with mixed tables and materialized views as tables."""
    config = [
        DuplicateDBServiceConfig(db_schema="public", tbl_view="users", is_view=False),
        DuplicateDBServiceConfig(db_schema="public", tbl_view="orders", is_view=False),
        DuplicateDBServiceConfig(
            db_schema="public", tbl_view="user_stats", is_view=True
        ),
        DuplicateDBServiceConfig(
            db_schema="analytics", tbl_view="reports", is_view=True
        ),
    ]

    dup_service = DuplicateDBService()

    # Mock table methods (now used for both tables and materialized views as tables)
    dup_service.generate_create_table_statement = AsyncMock()
    dup_service.create_table = AsyncMock()
    dup_service.copy_table_data = AsyncMock()

    # Mock materialized view as table method
    dup_service.copy_materialized_view_as_table = AsyncMock()

    mock_source_db = MagicMock(spec=IPostgresDBService)
    mock_target_db = MagicMock(spec=IPostgresDBService)

    await dup_service.duplicate(mock_source_db, mock_target_db, config)

    # Verify regular tables were processed (2 regular tables)
    assert dup_service.generate_create_table_statement.call_count == 2
    assert dup_service.create_table.call_count == 2
    assert dup_service.copy_table_data.call_count == 2

    # Verify materialized views were processed as tables (2 materialized views)
    assert dup_service.copy_materialized_view_as_table.call_count == 2

    # Check the specific calls for materialized views as tables
    dup_service.copy_materialized_view_as_table.assert_any_call(
        mock_source_db, mock_target_db, "public", "user_stats"
    )
    dup_service.copy_materialized_view_as_table.assert_any_call(
        mock_source_db, mock_target_db, "analytics", "reports"
    )


@pytest.mark.asyncio
async def test_duplicate():
    config = [
        DuplicateDBServiceConfig(db_schema="public", tbl_view="test1"),
        DuplicateDBServiceConfig(db_schema="public", tbl_view="test2"),
    ]

    dup_service = DuplicateDBService()
    dup_service.generate_create_table_statement = AsyncMock()
    dup_service.create_table = AsyncMock()
    dup_service.copy_table_data = AsyncMock()

    mock_source_db = MagicMock(spec=IPostgresDBService)
    mock_target_db = MagicMock(spec=IPostgresDBService)

    await dup_service.duplicate(mock_source_db, mock_target_db, config)

    assert dup_service.generate_create_table_statement.call_count == 2
    assert dup_service.create_table.call_count == 2
    assert dup_service.copy_table_data.call_count == 2


@pytest.mark.asyncio
async def test_generate_create_table_from_materialized_view_statement():
    """Test generate CREATE TABLE statement from materialized view structure."""
    mock_db = MagicMock(spec=IPostgresDBService)
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=None)

    # Mock the two sequential query calls
    mock_db.query = AsyncMock(
        side_effect=[
            # First call: get materialized view definition
            [{"definition": "SELECT id, user_count, created_at FROM users"}],
            # Second call: get table structure from temporary view
            [
                {
                    "create_statement": (
                        "CREATE TABLE public.user_stats (id integer NOT NULL, "
                        "user_count integer, created_at timestamp);"
                    )
                }
            ],
        ]
    )

    # Mock execute for temporary view creation and cleanup
    mock_db.execute = AsyncMock()

    dup_service = DuplicateDBService()
    result = await dup_service.generate_create_table_from_materialized_view_statement(
        mock_db, "public", "user_stats"
    )

    expected = (
        "CREATE TABLE public.user_stats (id integer NOT NULL, "
        "user_count integer, created_at timestamp);"
    )
    assert result == expected

    # Verify the sequence of calls
    assert mock_db.query.call_count == 2
    assert mock_db.execute.call_count == 2  # temp view creation + cleanup


@pytest.mark.asyncio
async def test_generate_create_table_from_materialized_view_statement_not_found():
    """Test error when materialized view is not found."""
    mock_db = MagicMock(spec=IPostgresDBService)
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=None)
    # Return empty list for materialized view definition query
    mock_db.query = AsyncMock(return_value=[])

    dup_service = DuplicateDBService()

    with pytest.raises(ValueError, match="not found in source database"):
        await dup_service.generate_create_table_from_materialized_view_statement(
            mock_db, "public", "nonexistent_view"
        )


@pytest.mark.asyncio
async def test_generate_create_table_from_materialized_view_statement_no_definition():
    """Test error when materialized view is not found."""
    mock_db = MagicMock(spec=IPostgresDBService)
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=None)
    # Mock the two sequential query calls
    mock_db.query = AsyncMock(
        side_effect=[
            # First call: get materialized view definition
            [{"definition": "SELECT id, user_count, created_at FROM users"}],
            # Second call: get table structure from temporary view
            [],
        ]
    )

    dup_service = DuplicateDBService()

    with pytest.raises(
        ValueError, match="Could not analyze structure of materialized view"
    ):
        await dup_service.generate_create_table_from_materialized_view_statement(
            mock_db, "public", "no_definition_view"
        )


@pytest.mark.asyncio
async def test_copy_materialized_view_as_table():
    """Test copying materialized view as a regular table."""
    mock_source_db = MagicMock(spec=IPostgresDBService)
    mock_target_db = MagicMock(spec=IPostgresDBService)

    dup_service = DuplicateDBService()

    # Mock the methods that will be called
    dup_service.generate_create_table_from_materialized_view_statement = AsyncMock(
        return_value="CREATE TABLE public.user_stats (id integer, name text);"
    )
    dup_service.create_table = AsyncMock()
    dup_service.copy_table_data = AsyncMock()

    await dup_service.copy_materialized_view_as_table(
        mock_source_db, mock_target_db, "public", "user_stats"
    )

    # Verify all methods were called correctly
    dup_service.generate_create_table_from_materialized_view_statement.assert_called_once_with(  # noqa: E501
        mock_source_db, "public", "user_stats"
    )
    dup_service.create_table.assert_called_once_with(
        mock_target_db,
        "public",
        "user_stats",
        "CREATE TABLE public.user_stats (id integer, name text);",
    )
    dup_service.copy_table_data.assert_called_once_with(
        mock_source_db, mock_target_db, "public", "user_stats"
    )
