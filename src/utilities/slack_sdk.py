from __future__ import annotations

from asyncio import Queue
from dataclasses import dataclass
from http import HTTPStatus
from itertools import chain
from logging import NOTSET, Handler, LogRecord
from typing import TYPE_CHECKING, override

from slack_sdk.webhook.async_client import AsyncWebhookClient

from utilities.asyncio import QueueProcessor, sleep_dur
from utilities.datetime import MINUTE, SECOND, datetime_duration_to_float
from utilities.functools import cache
from utilities.math import safe_round
from utilities.version import GetVersionError

if TYPE_CHECKING:
    from collections.abc import Callable

    from slack_sdk.webhook import WebhookResponse

    from utilities.types import Duration


_TIMEOUT = MINUTE
_SLEEP = SECOND


##


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
        timeout: Duration = _TIMEOUT,
        callback: Callable[[], None] | None = None,
        sleep: Duration = _SLEEP,
    ) -> None:
        QueueProcessor.__init__(  # QueueProcessor first
            self, queue_type=queue_type, queue_max_size=queue_max_size
        )
        QueueProcessor.__post_init__(self)
        Handler.__init__(self, level=level)
        self.url = url
        self.timeout = timeout
        self.callback = callback
        self.sleep = sleep

    @override
    def emit(self, record: LogRecord) -> None:
        try:
            self.enqueue(self.format(record))
        except GetVersionError:  # pragma: no cover
            raise
        except Exception:  # noqa: BLE001  # pragma: no cover
            self.handleError(record)

    @override
    async def _run(self, item: str, /) -> None:
        """Run the handler."""
        items = list(chain([item], await self._get_items_nowait()))
        text = "\n".join(items)
        await send_to_slack(self.url, text, timeout=self.timeout)  # pragma: no cover
        if self.callback is not None:  # pragma: no cover
            self.callback()
        await sleep_dur(duration=self.sleep)  # pragma: no cover


##


async def send_to_slack(
    url: str, text: str, /, *, timeout: Duration = _TIMEOUT
) -> None:
    """Send a message via Slack."""
    client = _get_client(url, timeout=timeout)
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
