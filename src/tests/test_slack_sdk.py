from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING

from aiohttp import InvalidUrlClientError
from pytest import mark, raises
from slack_sdk.webhook.async_client import AsyncWebhookClient

from utilities.asyncio import timeout_dur
from utilities.datetime import MINUTE
from utilities.os import get_env_var
from utilities.pytest import throttle
from utilities.slack_sdk import SlackHandler, _get_client, send_to_slack

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path


class TestGetClient:
    def test_main(self) -> None:
        client = _get_client("url")
        assert isinstance(client, AsyncWebhookClient)


class TestSendToSlack:
    async def test_main(self) -> None:
        with raises(InvalidUrlClientError, match="url"):
            await send_to_slack("url", "message")

    @mark.skipif(get_env_var("SLACK", nullable=True) is None, reason="'SLACK' not set")
    @throttle(duration=5 * MINUTE)
    async def test_real(self) -> None:
        url = get_env_var("SLACK")
        await send_to_slack(
            url, f"message from {TestSendToSlack.test_real.__qualname__}"
        )


class TestSlackHandler:
    async def test_main(self, *, tmp_path: Path) -> None:
        messages: Sequence[str] = []

        async def sender(_: str, text: str, /) -> None:
            messages.append(text)

        logger = getLogger(str(tmp_path))
        logger.addHandler(
            handler := SlackHandler("url", sleep_core=0.05, sender=sender)
        )
        async with timeout_dur(duration=1.0), handler:
            logger.warning("message")
        assert messages == ["message"]

    @mark.skipif(get_env_var("SLACK", nullable=True) is None, reason="'SLACK' not set")
    @throttle(duration=5 * MINUTE)
    async def test_real(self, *, tmp_path: Path) -> None:
        url = get_env_var("SLACK")
        logger = getLogger(str(tmp_path))
        logger.addHandler(handler := SlackHandler(url, sleep_core=0.05))
        async with timeout_dur(duration=1.0), handler:
            for i in range(10):
                logger.warning(
                    "message %d from %s", i, TestSlackHandler.test_real.__qualname__
                )
