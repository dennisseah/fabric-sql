from unittest.mock import AsyncMock, MagicMock

import pytest

from fabric_sql.protocols.i_target_database import ITargetDatabase
from fabric_sql.services.database_definitions import DatabaseDefinitions


def tet_post_init() -> None:
    svc = DatabaseDefinitions(target_db=MagicMock(), view_definitions={})
    assert svc.definitions is not None


@pytest.mark.asyncio
async def test_get_view_schemas() -> None:
    mock_target_db = MagicMock(spec=ITargetDatabase)
    mock_target_db.show_view_definition = AsyncMock(
        return_value=[{"schema_name": "public", "view_name": "example_view"}]
    )

    svc = DatabaseDefinitions(target_db=mock_target_db, view_definitions={})
    await svc.get_view_schemas()
    assert len(svc.view_definitions) > 0


@pytest.mark.asyncio
async def test_get_view_schemas_already_fetch() -> None:
    mock_target_db = MagicMock(spec=ITargetDatabase)
    mock_target_db.show_view_definition = AsyncMock()

    svc = DatabaseDefinitions(target_db=mock_target_db, view_definitions={"abc": []})
    await svc.get_view_schemas()
    assert len(svc.view_definitions) > 0

    mock_target_db.show_view_definition.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_definitions() -> None:
    mock_target_db = MagicMock(spec=ITargetDatabase)
    mock_target_db.show_view_definition = AsyncMock(
        return_value=[{"schema_name": "public", "view_name": "example_view"}]
    )

    svc = DatabaseDefinitions(target_db=mock_target_db, view_definitions={})
    definitions = await svc.get_definitions()
    assert len(definitions) > 0


def test_get_table_definitions() -> None:
    svc = DatabaseDefinitions(target_db=MagicMock(), view_definitions={})
    tables = svc.get_table_definitions()
    assert len(tables) > 0


def test_get_view_definitions() -> None:
    svc = DatabaseDefinitions(target_db=MagicMock(), view_definitions={})
    views = svc.get_view_definitions()
    assert len(views) > 0
