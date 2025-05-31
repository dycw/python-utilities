from __future__ import annotations

from asyncio import sleep
from dataclasses import dataclass
from http import HTTPStatus
from logging import NOTSET, Handler, LogRecord
from typing import TYPE_CHECKING, Self, override

from slack_sdk.webhook.async_client import AsyncWebhookClient

from utilities.asyncio import (
    InfiniteQueueLooper,
    Looper,
    LooperTimeoutError,
    timeout_dur,
)
from utilities.dataclasses import replace_non_sentinel
from utilities.datetime import MINUTE, SECOND, datetime_duration_to_float
from utilities.functools import cache
from utilities.math import safe_round
from utilities.sentinel import Sentinel, sentinel

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

    from slack_sdk.webhook import WebhookResponse

    from utilities.types import Coroutine1, Duration


_TIMEOUT: Duration = MINUTE


##


_SLEEP: Duration = SECOND


async def _send_adapter(url: str, text: str, /) -> None:
    await send_to_slack(url, text)  # pragma: no cover


@dataclass(init=False, unsafe_hash=True)
class SlackHandler(Handler, InfiniteQueueLooper[None, str]):
    """Handler for sending messages to Slack."""

    @override
    def __init__(
        self,
        url: str,
        /,
        *,
        level: int = NOTSET,
        sleep_core: Duration = _SLEEP,
        sleep_restart: Duration = _SLEEP,
        sender: Callable[[str, str], Coroutine1[None]] = _send_adapter,
        timeout: Duration = _TIMEOUT,
    ) -> None:
        InfiniteQueueLooper.__init__(self)  # InfiniteQueueLooper first
        InfiniteQueueLooper.__post_init__(self)
        Handler.__init__(self, level=level)  # Handler next
        self.url = url
        self.sender = sender
        self.timeout = timeout
        self.sleep_core = sleep_core
        self.sleep_restart = sleep_restart

    @override
    def emit(self, record: LogRecord) -> None:
        try:
            self.put_right_nowait(self.format(record))
        except Exception:  # noqa: BLE001  # pragma: no cover
            self.handleError(record)

    @override
    async def _process_queue(self) -> None:
        messages = self._queue.get_all_nowait()
        text = "\n".join(messages)
        async with timeout_dur(duration=self.timeout):
            await self.sender(self.url, text)


@dataclass(init=False, unsafe_hash=True)
class SlackHandlerService(Handler, Looper[str]):
    """Service to send messages to Slack."""

    @override
    def __init__(
        self,
        *,
        url: str,
        auto_start: bool = False,
        freq: Duration = SECOND,
        backoff: Duration = SECOND,
        logger: str | None = None,
        timeout: Duration | None = None,
        timeout_error: type[Exception] = LooperTimeoutError,
        _debug: bool = False,
        level: int = NOTSET,
        sender: Callable[[str, str], Coroutine1[None]] = _send_adapter,
        send_timeout: Duration = SECOND,
    ) -> None:
        Looper.__init__(  # Looper first
            self,
            auto_start=auto_start,
            freq=freq,
            backoff=backoff,
            logger=logger,
            timeout=timeout,
            timeout_error=timeout_error,
            _debug=_debug,
        )
        Looper.__post_init__(self)
        Handler.__init__(self, level=level)  # Handler next
        self.url = url
        self.sender = sender
        self.send_timeout = send_timeout

    @override
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        await self.run_until_empty()
        await super().__aexit__(exc_type, exc_value, traceback)

    async def run_until_empty(self) -> None:
        """Run the core function until the queue is empty."""
        while not self.empty():
            await self.core()
            if not self.empty():
                await sleep(self._freq)

    @override
    def emit(self, record: LogRecord) -> None:
        fmtted = self.format(record)
        try:
            self.put_right_nowait(fmtted)
        except Exception:  # noqa: BLE001  # pragma: no cover
            self.handleError(record)

    @override
    async def core(self) -> None:
        await super().core()
        if self.empty():
            return
        text = "\n".join(self.get_all_nowait())
        async with timeout_dur(duration=self.send_timeout):
            await self.sender(self.url, text)

    @override
    def replace(
        self,
        *,
        auto_start: bool | Sentinel = sentinel,
        freq: Duration | Sentinel = sentinel,
        backoff: Duration | Sentinel = sentinel,
        logger: str | None | Sentinel = sentinel,
        timeout: Duration | None | Sentinel = sentinel,
        timeout_error: type[Exception] | Sentinel = sentinel,
        _debug: bool | Sentinel = sentinel,
    ) -> Self:
        """Replace elements of the looper."""
        return replace_non_sentinel(
            self,
            url=self.url,
            auto_start=auto_start,
            freq=freq,
            backoff=backoff,
            logger=logger,
            timeout=timeout,
            timeout_error=timeout_error,
            _debug=_debug,
        )


##


async def send_to_slack(
    url: str, text: str, /, *, timeout: Duration = _TIMEOUT
) -> None:
    """Send a message via Slack."""
    client = _get_client(url, timeout=timeout)
    async with timeout_dur(duration=timeout):
        response = await client.send(text=text)
    if response.status_code != HTTPStatus.OK:  # pragma: no cover
        raise SendToSlackError(text=text, response=response)


@dataclass(kw_only=True, slots=True)
class SendToSlackError(Exception):
    text: str
    response: WebhookResponse

    @override
    def __str__(self) -> str:
        code = self.response.status_code  # pragma: no cover
        phrase = HTTPStatus(code).phrase  # pragma: no cover
        return f"Error sending to Slack:\n\n{self.text}\n\n{code}: {phrase}"  # pragma: no cover


@cache
def _get_client(url: str, /, *, timeout: Duration = _TIMEOUT) -> AsyncWebhookClient:
    """Get the Slack client."""
    timeout_use = safe_round(datetime_duration_to_float(timeout))
    return AsyncWebhookClient(url, timeout=timeout_use)


__all__ = ["SendToSlackError", "SlackHandler", "SlackHandlerService", "send_to_slack"]
