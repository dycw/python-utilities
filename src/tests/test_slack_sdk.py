from __future__ import annotations

from asyncio import sleep
from logging import DEBUG, getLogger
from typing import TYPE_CHECKING

from aiohttp import InvalidUrlClientError
from pytest import mark, param, raises
from slack_sdk.webhook.async_client import AsyncWebhookClient

from utilities.slack_sdk import SlackHandler, _get_client, send_to_slack

if TYPE_CHECKING:
    from pathlib import Path


class TestGetClient:
    def test_main(self) -> None:
        client = _get_client("url")
        assert isinstance(client, AsyncWebhookClient)


class TestSendToSlack:
    async def test_main(self) -> None:
        with raises(InvalidUrlClientError, match="url"):
            await send_to_slack("url", "message")


class TestSlackHandler:
    async def test_main(self, *, tmp_path: Path) -> None:
        logger = getLogger(str(tmp_path))
        logger.setLevel(DEBUG)
        handler = SlackHandler("url")
        handler.setLevel(DEBUG)
        logger.addHandler(handler)
        logger.debug("message")
        await sleep(0.1)

    @mark.parametrize("cancel", [param(True), param(False)], ids=str)
    async def test_send(self, *, tmp_path: Path, cancel: bool) -> None:
        logger = getLogger(f"{tmp_path}_{cancel}")
        logger.setLevel(DEBUG)
        handler = SlackHandler("url")
        handler.setLevel(DEBUG)
        logger.addHandler(handler)
        logger.debug("message")
        await handler.send(cancel=cancel)
        if cancel:
            await sleep(0.1)
            assert handler._task.done()
