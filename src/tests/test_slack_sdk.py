from __future__ import annotations

from aiohttp import InvalidUrlClientError
from pytest import mark, raises
from slack_sdk.webhook.async_client import AsyncWebhookClient

from utilities.os import get_env_var
from utilities.pytest import throttle
from utilities.slack_sdk import _get_async_client, send_to_slack, send_to_slack_async
from utilities.whenever import MINUTE


class TestGetClient:
    def test_main(self) -> None:
        client = _get_async_client("url")
        assert isinstance(client, AsyncWebhookClient)


class TestSendToSlack:
    def test_sync(self) -> None:
        with raises(ValueError, match="unknown url type"):
            send_to_slack("url", "message")

    async def test_async(self) -> None:
        with raises(InvalidUrlClientError, match="url"):
            await send_to_slack_async("url", "message")

    @mark.skipif(get_env_var("SLACK", nullable=True) is None, reason="'SLACK' not set")
    @throttle(delta=5 * MINUTE)
    async def test_real(self) -> None:
        url = get_env_var("SLACK")
        await send_to_slack_async(
            url, f"message from {TestSendToSlack.test_real.__qualname__}"
        )
