import asyncio

from autogen_agentchat.agents import UserProxyAgent
from autogen_agentchat.base import TaskResult
from autogen_agentchat.conditions import TextMentionTermination
from autogen_agentchat.teams import SelectorGroupChat
from autogen_ext.models.openai import AzureOpenAIChatCompletionClient

from fabric_sql.agents.compliance_agent import Agent as ComplianceAgent
from fabric_sql.agents.db_query_agent import Agent as DbQueryAgent
from fabric_sql.hosting import container
from fabric_sql.protocols.i_chat_client import IChatClient

chat_client = container[IChatClient]

selector_prompt = """Select an agent to perform task which best fits the task.

The task cannot be get SQL query, execute SQL query, or access database directly.

{roles}

Current conversation context:
{history}

Read the above conversation, then select an agent from {participants} to perform the
next task.
Make sure the planner agent has assigned tasks before other agents start working.
Only select one agent.
"""


async def get_team(llm_client: AzureOpenAIChatCompletionClient) -> SelectorGroupChat:
    compliance_agent = await ComplianceAgent().get_agent(llm_client)
    db_query_agent = await DbQueryAgent().get_agent(llm_client)
    user_proxy = UserProxyAgent("user_proxy", input_func=input)

    termination = TextMentionTermination("TERMINATE")
    return SelectorGroupChat(
        [
            compliance_agent,
            db_query_agent,
            user_proxy,
        ],
        model_client=llm_client,
        termination_condition=termination,
        selector_prompt=selector_prompt,
        allow_repeated_speaker=False,
    )


async def main() -> None:
    llm_client = chat_client.get_model_client()
    team = await get_team(llm_client=llm_client)

    async for message in team.run_stream(
        task="What are the affected policies in the last 7 days?",
    ):
        if type(message) is not TaskResult:
            if message.source != "user_proxy":  # type: ignore
                print(message.content)  # type: ignore


if __name__ == "__main__":
    asyncio.run(main())
