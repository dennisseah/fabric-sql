from typing import Protocol

from autogen_ext.models.openai import AzureOpenAIChatCompletionClient


class IChatClient(Protocol):
    def get_model_client(self) -> AzureOpenAIChatCompletionClient: ...
