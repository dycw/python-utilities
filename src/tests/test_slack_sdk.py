from __future__ import annotations

from os import environ

from aiohttp import InvalidUrlClientError
from pytest import mark, raises
from slack_sdk.webhook.async_client import AsyncWebhookClient

from utilities.constants import MINUTE
from utilities.pytest import throttle_test
from utilities.slack_sdk import _get_async_client, send_to_slack, send_to_slack_async


class TestGetClient:
    def test_main(self) -> None:
        client = _get_async_client("url")
        assert isinstance(client, AsyncWebhookClient)


class TestSendToSlack:
    def test_sync(self) -> None:
        with raises(ValueError, match=r"unknown url type"):
            send_to_slack("url", "message")

    async def test_async(self) -> None:
        with raises(InvalidUrlClientError, match=r"url"):
            await send_to_slack_async("url", "message")

    @mark.skipif("SLACK" not in environ, reason="'SLACK' not set")
    @throttle_test(duration=5 * MINUTE)
    async def test_real(self) -> None:
        url = environ["SLACK"]
        await send_to_slack_async(
            url, f"message from {TestSendToSlack.test_real.__qualname__}"
        )
