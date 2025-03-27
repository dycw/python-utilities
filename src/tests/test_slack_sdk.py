from __future__ import annotations

from asyncio import sleep
from logging import DEBUG, getLogger
from typing import TYPE_CHECKING

from aiohttp import InvalidUrlClientError
from pytest import mark, raises
from slack_sdk.webhook.async_client import AsyncWebhookClient

from utilities.datetime import MINUTE
from utilities.os import get_env_var
from utilities.pytest import throttle
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

    @mark.skipif(
        get_env_var("SLACK", case_sensitive=False, nullable=True) is None,
        reason="'SLACK' not set",
    )
    @throttle(duration=5 * MINUTE)
    async def test_real(self) -> None:
        url = get_env_var("SLACK", case_sensitive=False)
        await send_to_slack(
            url, f"message from {TestSendToSlack.test_real.__qualname__}"
        )


class TestSlackHandler:
    async def test_main(self, *, tmp_path: Path) -> None:
        logger = getLogger(str(tmp_path))
        logger.setLevel(DEBUG)
        handler = SlackHandler("url")
        handler.setLevel(DEBUG)
        logger.addHandler(handler)
        await handler.start()
        logger.debug("message from %s", TestSlackHandler.test_main.__qualname__)
        await sleep(0.1)

    @mark.skipif(
        get_env_var("SLACK", case_sensitive=False, nullable=True) is None,
        reason="'SLACK' not set",
    )
    @throttle(duration=5 * MINUTE)
    async def test_real(self, *, tmp_path: Path) -> None:
        logger = getLogger(str(tmp_path))
        logger.setLevel(DEBUG)
        url = get_env_var("SLACK", case_sensitive=False)
        handler = SlackHandler(url)
        handler.setLevel(DEBUG)
        logger.addHandler(handler)
        await handler.start()
        for i in range(10):
            logger.debug(
                "message %d from %s", i, TestSlackHandler.test_real.__qualname__
            )
        await sleep(0.1)
