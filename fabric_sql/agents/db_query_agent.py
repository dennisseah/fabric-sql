from autogen_agentchat.agents import AssistantAgent
from autogen_ext.models.openai import AzureOpenAIChatCompletionClient
from tabulate import tabulate

from fabric_sql.agents.i_agent import IAgent
from fabric_sql.hosting import container
from fabric_sql.protocols.i_database_definitions import IDatabaseDefinitions
from fabric_sql.protocols.i_target_database import ITargetDatabase

db_definition_service = container[IDatabaseDefinitions]
target_db = container[ITargetDatabase]


async def query_tool(query: str) -> str:
    """Execute the SQL query using the target database and return the result as a
    string.
    """
    async with target_db:
        result = await target_db.query(query)
        return tabulate(result, headers="keys", tablefmt="grid")  # type: ignore


class Agent(IAgent):
    async def system_message(self) -> str:
        return (
            "You are a helpful assistant that take a generated SQL query and execute "
            "it with the tool provided. Only respond with the SQL result, no extra "
            "text."
        )

    async def get_agent(
        self, llm_client: AzureOpenAIChatCompletionClient
    ) -> AssistantAgent:
        return AssistantAgent(
            "db_query_agent",
            model_client=llm_client,
            description="Database Query Agent.",
            tools=[query_tool],
            system_message=await self.system_message(),
        )
