from __future__ import annotations

from slack_sdk.webhook.async_client import AsyncWebhookClient

from utilities.slack_sdk import _get_client, send_to_slack


class TestGetClient:
    def test_main(self) -> None:
        client = _get_client("url")
        assert isinstance(client, AsyncWebhookClient)


class TestSendToSlack:
    async def test_main(self) -> None:
        await send_to_slack("message", _get_client)
