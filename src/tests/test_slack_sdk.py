from __future__ import annotations

from asyncio import sleep
from logging import getLogger
from re import search
from typing import TYPE_CHECKING

from aiohttp import InvalidUrlClientError
from pytest import mark, raises
from slack_sdk.webhook.async_client import AsyncWebhookClient

from utilities.datetime import MINUTE
from utilities.iterables import one
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
            await sleep(0.01)
            messages.append(text)

        logger = getLogger(str(tmp_path))
        logger.addHandler(handler := SlackHandler("url", sender=sender))
        async with handler:
            logger.warning("message")
        assert messages == ["message"]

    async def test_callback_failure(self, *, tmp_path: Path) -> None:
        messages: Sequence[str] = []
        failures: Sequence[tuple[str, Exception]] = []

        async def sender(_: str, text: str, /) -> None:
            await sleep(0.1)
            messages.append(text)

        def callback(text: str, error: Exception, /) -> None:
            failures.append((text, error))

        logger = getLogger(str(tmp_path))
        logger.addHandler(
            handler := SlackHandler(
                "url", sender=sender, timeout=0.01, callback_failure=callback
            )
        )
        async with handler:
            logger.warning("message")
        assert messages == []
        assert len(failures) == 1
        failure = one(failures)
        assert failure[0] == "message"
        assert isinstance(failure[1], TimeoutError)

    async def test_callback_success(self, *, tmp_path: Path) -> None:
        messages: Sequence[str] = []
        successes: Sequence[str] = []

        async def sender(_: str, text: str, /) -> None:
            await sleep(0.01)
            messages.append(text)

        def callback(text: str, /) -> None:
            successes.append(text)

        logger = getLogger(str(tmp_path))
        logger.addHandler(
            handler := SlackHandler("url", sender=sender, callback_success=callback)
        )
        async with handler:
            logger.warning("message")
        assert messages == ["message"]
        assert successes == ["message"]

    async def test_callback_final(self, *, tmp_path: Path) -> None:
        messages: Sequence[str] = []
        finals: Sequence[str] = []

        async def sender(_: str, text: str, /) -> None:
            if search("slow", text):
                await sleep(0.1)
            else:
                await sleep(0.01)
            messages.append(text)

        def callback(text: str, /) -> None:
            finals.append(text)

        logger = getLogger(str(tmp_path))
        logger.addHandler(
            handler := SlackHandler(
                "url", sender=sender, timeout=0.05, callback_final=callback, sleep=0.01
            )
        )
        async with handler:
            logger.warning("fast message")
            await sleep(0.1)
            logger.warning("slow message")
        assert messages == ["fast message"]
        assert finals == ["fast message", "slow message"]

    @mark.skipif(get_env_var("SLACK", nullable=True) is None, reason="'SLACK' not set")
    @throttle(duration=5 * MINUTE)
    async def test_real(self, *, tmp_path: Path) -> None:
        url = get_env_var("SLACK")
        logger = getLogger(str(tmp_path))
        logger.addHandler(handler := SlackHandler(url))
        async with handler:
            for i in range(10):
                logger.warning(
                    "message %d from %s", i, TestSlackHandler.test_real.__qualname__
                )
        await sleep(0.1)
