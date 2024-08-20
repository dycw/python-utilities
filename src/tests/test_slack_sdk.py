from __future__ import annotations

from slack_sdk.webhook import WebhookClient
from slack_sdk.webhook.async_client import AsyncWebhookClient

from utilities.slack_sdk import _get_client_async, _get_client_sync


class TestGetClient:
    def test_sync(self) -> None:
        client = _get_client_sync("url")
        assert isinstance(client, WebhookClient)

    def test_async(self) -> None:
        client = _get_client_async("url")
        assert isinstance(client, AsyncWebhookClient)
