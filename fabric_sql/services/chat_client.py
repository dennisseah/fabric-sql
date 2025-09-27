from dataclasses import dataclass

from autogen_ext.models.openai import AzureOpenAIChatCompletionClient
from azure.identity import DefaultAzureCredential
from lagom.environment import Env

from fabric_sql.protocols.i_chat_client import IChatClient


class ChatClientEnv(Env):
    azure_openai_endpoint: str
    azure_openai_api_key: str | None = None
    azure_openai_api_version: str
    azure_openai_model_name: str


@dataclass
class ChatClient(IChatClient):
    env: ChatClientEnv

    def get_client(self) -> AzureOpenAIChatCompletionClient:
        if self.env.azure_openai_api_key:
            return AzureOpenAIChatCompletionClient(
                azure_endpoint=self.env.azure_openai_endpoint,
                api_key=self.env.azure_openai_api_key,
                model=self.env.azure_openai_model_name,
                api_version=self.env.azure_openai_api_version,
            )

        azure_ad_token = (
            DefaultAzureCredential()
            .get_token("https://cognitiveservices.azure.com/.default")
            .token
        )

        return AzureOpenAIChatCompletionClient(
            azure_endpoint=self.env.azure_openai_endpoint,
            azure_ad_token=azure_ad_token,
            model=self.env.azure_openai_model_name,
            api_version=self.env.azure_openai_api_version,
        )

    def __post_init__(self) -> None:
        self.model_client = self.get_client()

    def get_model_client(self) -> AzureOpenAIChatCompletionClient:
        return self.model_client
