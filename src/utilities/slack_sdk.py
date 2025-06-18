from __future__ import annotations

from dataclasses import dataclass
from http import HTTPStatus
from logging import NOTSET, Handler, LogRecord
from typing import TYPE_CHECKING, Any, Self, override

from slack_sdk.webhook.async_client import AsyncWebhookClient

from utilities.asyncio import Looper, timeout_td
from utilities.functools import cache
from utilities.sentinel import Sentinel, sentinel
from utilities.whenever import MINUTE, SECOND

if TYPE_CHECKING:
    from collections.abc import Callable

    from slack_sdk.webhook import WebhookResponse
    from whenever import TimeDelta

    from utilities.types import Coro


_TIMEOUT: TimeDelta = MINUTE


##


async def _send_adapter(url: str, text: str, /) -> None:
    await send_to_slack(url, text)  # pragma: no cover


@dataclass(init=False, unsafe_hash=True)
class SlackHandlerService(Handler, Looper[str]):
    """Service to send messages to Slack."""

    @override
    def __init__(
        self,
        *,
        url: str,
        auto_start: bool = False,
        empty_upon_exit: bool = True,
        freq: TimeDelta = SECOND,
        backoff: TimeDelta = SECOND,
        logger: str | None = None,
        timeout: TimeDelta | None = None,
        _debug: bool = False,
        level: int = NOTSET,
        sender: Callable[[str, str], Coro[None]] = _send_adapter,
        send_timeout: TimeDelta = SECOND,
    ) -> None:
        Looper.__init__(  # Looper first
            self,
            auto_start=auto_start,
            freq=freq,
            empty_upon_exit=empty_upon_exit,
            backoff=backoff,
            logger=logger,
            timeout=timeout,
            _debug=_debug,
        )
        Looper.__post_init__(self)
        Handler.__init__(self, level=level)  # Handler next
        self.url = url
        self.sender = sender
        self.send_timeout = send_timeout

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
        async with timeout_td(self.send_timeout):
            await self.sender(self.url, text)

    @override
    def replace(
        self,
        *,
        auto_start: bool | Sentinel = sentinel,
        empty_upon_exit: bool | Sentinel = sentinel,
        freq: TimeDelta | Sentinel = sentinel,
        backoff: TimeDelta | Sentinel = sentinel,
        logger: str | None | Sentinel = sentinel,
        timeout: TimeDelta | None | Sentinel = sentinel,
        _debug: bool | Sentinel = sentinel,
        **kwargs: Any,
    ) -> Self:
        """Replace elements of the looper."""
        return super().replace(
            url=self.url,
            auto_start=auto_start,
            empty_upon_exit=empty_upon_exit,
            freq=freq,
            backoff=backoff,
            logger=logger,
            timeout=timeout,
            _debug=_debug,
            **kwargs,
        )


##


async def send_to_slack(
    url: str, text: str, /, *, timeout: TimeDelta = _TIMEOUT
) -> None:
    """Send a message via Slack."""
    client = _get_client(url, timeout=timeout)
    async with timeout_td(timeout):
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
def _get_client(url: str, /, *, timeout: TimeDelta = _TIMEOUT) -> AsyncWebhookClient:
    """Get the Slack client."""
    return AsyncWebhookClient(url, timeout=round(timeout.in_seconds()))


__all__ = ["SendToSlackError", "SlackHandlerService", "send_to_slack"]
