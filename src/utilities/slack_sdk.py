from __future__ import annotations

from asyncio import Queue
from dataclasses import dataclass
from http import HTTPStatus
from itertools import chain
from logging import NOTSET, Handler, LogRecord
from typing import TYPE_CHECKING, override

from slack_sdk.webhook.async_client import AsyncWebhookClient

from utilities.asyncio import QueueProcessor, sleep_dur, timeout_dur
from utilities.datetime import MINUTE, SECOND, datetime_duration_to_float
from utilities.functools import cache
from utilities.math import safe_round

if TYPE_CHECKING:
    from collections.abc import Callable

    from slack_sdk.webhook import WebhookResponse

    from utilities.types import Coroutine1, Duration


_TIMEOUT: Duration = MINUTE


##


_SLEEP: Duration = SECOND


async def _send_adapter(url: str, text: str, /) -> None:
    await send_to_slack(url, text)  # pragma: no cover


@dataclass(init=False, order=True, unsafe_hash=True)
class SlackHandler(Handler, QueueProcessor[str]):
    """Handler for sending messages to Slack."""

    @override
    def __init__(
        self,
        url: str,
        /,
        *,
        level: int = NOTSET,
        queue_type: type[Queue[str]] = Queue,
        queue_max_size: int | None = None,
        sender: Callable[[str, str], Coroutine1[None]] = _send_adapter,
        timeout: Duration = _TIMEOUT,
        callback_failure: Callable[[str, Exception], None] | None = None,
        callback_success: Callable[[str], None] | None = None,
        callback_final: Callable[[str], None] | None = None,
        sleep: Duration = _SLEEP,
    ) -> None:
        QueueProcessor.__init__(  # QueueProcessor first
            self, queue_type=queue_type, queue_max_size=queue_max_size
        )
        QueueProcessor.__post_init__(self)
        Handler.__init__(self, level=level)
        self.url = url
        self.sender = sender
        self.timeout = timeout
        self.callback_failure = callback_failure
        self.callback_success = callback_success
        self.callback_final = callback_final
        self.sleep = sleep

    @override
    def emit(self, record: LogRecord) -> None:
        try:
            self.enqueue(self.format(record))
        except Exception:  # noqa: BLE001  # pragma: no cover
            self.handleError(record)

    @override
    async def _process_item(self, item: str, /) -> None:
        """Process the first item."""
        items = list(chain([item], await self._get_items_nowait()))
        text = "\n".join(items)
        try:
            async with timeout_dur(duration=self.timeout):
                await self.sender(self.url, text)
        except Exception as error:  # noqa: BLE001
            if self.callback_failure is not None:
                self.callback_failure(text, error)
        else:
            if self.callback_success is not None:
                self.callback_success(text)
        finally:
            if self.callback_final is not None:
                self.callback_final(text)
            await sleep_dur(duration=self.sleep)


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


__all__ = ["SendToSlackError", "SlackHandler", "send_to_slack"]
