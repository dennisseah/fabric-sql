from typing import Protocol

from autogen_agentchat.agents import AssistantAgent
from autogen_ext.models.openai import AzureOpenAIChatCompletionClient


class IAgent(Protocol):
    async def get_agent(
        self, llm_client: AzureOpenAIChatCompletionClient
    ) -> AssistantAgent:
        """
        Get an instance of AssistantAgent.

        :param llm_client: The language model client.
        :return: An instance of AssistantAgent.
        """
        ...
