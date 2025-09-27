from pytest_mock import MockerFixture

from fabric_sql.services.chat_client import ChatClient, ChatClientEnv


def test_get_client_with_api_key(mocker: MockerFixture):
    mocked_default_cred = mocker.patch(
        "fabric_sql.services.chat_client.DefaultAzureCredential"
    )
    mock_class = mocker.patch(
        "fabric_sql.services.chat_client.AzureOpenAIChatCompletionClient",
    )

    env = ChatClientEnv(
        azure_openai_endpoint="https://example.com",
        azure_openai_api_key="test_api_key",
        azure_openai_api_version="2023-03-15-preview",
        azure_openai_model_name="gpt-4",
    )
    ChatClient(env=env)

    mocked_default_cred.assert_not_called()
    mock_class.assert_called_once_with(
        azure_endpoint="https://example.com",
        api_key="test_api_key",
        model="gpt-4",
        api_version="2023-03-15-preview",
    )


def test_get_client_with_pwd(mocker: MockerFixture):
    mocked_default_cred = mocker.patch(
        "fabric_sql.services.chat_client.DefaultAzureCredential",
        return_value=mocker.MagicMock(
            get_token=mocker.MagicMock(return_value=mocker.MagicMock(token="pwd"))
        ),
    )
    mock_class = mocker.patch(
        "fabric_sql.services.chat_client.AzureOpenAIChatCompletionClient",
    )

    env = ChatClientEnv(
        azure_openai_endpoint="https://example.com",
        azure_openai_api_version="2023-03-15-preview",
        azure_openai_model_name="gpt-4",
    )
    ChatClient(env=env)

    mocked_default_cred.assert_called_once()
    mock_class.assert_called_once_with(
        azure_endpoint="https://example.com",
        azure_ad_token="pwd",
        model="gpt-4",
        api_version="2023-03-15-preview",
    )


def test_get_model_client(mocker: MockerFixture):
    mocker.patch("fabric_sql.services.chat_client.DefaultAzureCredential")
    mocker.patch(
        "fabric_sql.services.chat_client.AzureOpenAIChatCompletionClient",
    )

    env = ChatClientEnv(
        azure_openai_endpoint="https://example.com",
        azure_openai_api_key="test_api_key",
        azure_openai_api_version="2023-03-15-preview",
        azure_openai_model_name="gpt-4",
    )
    chat_client = ChatClient(env=env)
    assert chat_client.get_model_client() is not None
