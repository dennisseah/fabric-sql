"""Defines our top level DI container.
Utilizes the Lagom library for dependency injection, see more at:

- https://lagom-di.readthedocs.io/en/latest/
- https://github.com/meadsteve/lagom
"""

import logging
import os

from dotenv import load_dotenv
from lagom import Container, dependency_definition

from fabric_sql.protocols.i_source_database import ISourceDatabase
from fabric_sql.protocols.i_target_database import ITargetDatabase

load_dotenv(dotenv_path=".env")


container = Container()
"""The top level DI container for our application."""


# Register our dependencies ------------------------------------------------------------


@dependency_definition(container, singleton=True)
def logger() -> logging.Logger:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "ERROR"))
    logging.Formatter(fmt=" %(name)s :: %(levelname)-8s :: %(message)s")
    return logging.getLogger("langgraph_memory")


@dependency_definition(container, singleton=True)
def source_db() -> ISourceDatabase:
    from fabric_sql.services.source_database import SourceDatabase

    return container[SourceDatabase]


@dependency_definition(container, singleton=True)
def target_db() -> ITargetDatabase:
    from fabric_sql.services.target_database import TargetDatabase

    return container[TargetDatabase]
