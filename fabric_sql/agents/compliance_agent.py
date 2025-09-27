from autogen_agentchat.agents import AssistantAgent
from autogen_ext.models.openai import AzureOpenAIChatCompletionClient

from fabric_sql.agents.i_agent import IAgent
from fabric_sql.hosting import container
from fabric_sql.protocols.i_database_definitions import IDatabaseDefinitions

db_definition_service = container[IDatabaseDefinitions]


class Agent(IAgent):
    async def system_message(self) -> str:
        return f"""You are a helpful assistant that generates SQL queries based on natural language. Only respond with the SQL query, no extra text.
Followings are the columns, tables and views definitions in the database:"
{await db_definition_service.get_definitions()}

INSTRUCTIONS:
1. LIMIT the number of results to 100 rows only, unless specifically asked for more.
2. Include as many columns as possible in the SELECT statement, do not use "SELECT *".
3. Use standard SQL syntax compatible with PostgreSQL.
4. If the query involves date or time, use the current date and time as '2024-06-01'.
"""  # noqa E501

    async def get_agent(
        self, llm_client: AzureOpenAIChatCompletionClient
    ) -> AssistantAgent:
        return AssistantAgent(
            "security_compliance_agent",
            model_client=llm_client,
            description="Security Compliance Agent.",
            system_message=await self.system_message(),
        )
