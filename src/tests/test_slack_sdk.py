from __future__ import annotations

from asyncio import sleep
from logging import DEBUG, getLogger

from aiohttp import InvalidUrlClientError
from pytest import raises
from slack_sdk.webhook.async_client import AsyncWebhookClient

from utilities.slack_sdk import SlackHandler, _get_client, send_to_slack


class TestGetClient:
    def test_main(self) -> None:
        client = _get_client("url")
        assert isinstance(client, AsyncWebhookClient)


class TestSendToSlack:
    async def test_main(self) -> None:
        with raises(InvalidUrlClientError, match="url"):
            await send_to_slack("url", "message")


class TestSlackHandler:
    async def test_main(self) -> None:
        name = TestSlackHandler.test_main.__qualname__
        logger = getLogger(name)
        logger.setLevel(DEBUG)
        handler = SlackHandler("url")
        handler.setLevel(DEBUG)
        logger.addHandler(handler)
        logger.debug("message")
        await sleep(0.1)

    async def test_send(self) -> None:
        name = TestSlackHandler.test_send.__qualname__
        logger = getLogger(name)
        logger.setLevel(DEBUG)
        handler = SlackHandler("url")
        handler.setLevel(DEBUG)
        logger.addHandler(handler)
        logger.debug("message")
        await handler.send()
