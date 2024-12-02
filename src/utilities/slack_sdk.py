from __future__ import annotations

from asyncio import CancelledError, Lock, Queue, QueueEmpty, Task, get_event_loop
from contextlib import suppress
from dataclasses import dataclass
from http import HTTPStatus
from logging import NOTSET, Handler, LogRecord, getLogger
from typing import TYPE_CHECKING

from slack_sdk.webhook.async_client import AsyncWebhookClient
from typing_extensions import override

from utilities.datetime import MINUTE, duration_to_float
from utilities.functools import cache
from utilities.math import safe_round

if TYPE_CHECKING:
    from slack_sdk.webhook import WebhookResponse

    from utilities.types import Duration

_LOGGER = getLogger(__name__)
_TIMEOUT = MINUTE


class SlackHandler(Handler):
    """Handler for sending messages to Slack."""

    _url: str
    _timeout: Duration
    _lock: Lock
    _queue: Queue[str]
    _task: Task[None]

    @override
    def __init__(
        self, url: str, /, *, level: int = NOTSET, timeout: Duration = _TIMEOUT
    ) -> None:
        super().__init__(level=level)
        self._url = url
        self._timeout = timeout
        self._lock = Lock()
        self._queue = Queue()
        self._task = get_event_loop().create_task(self._process_queue())

    @override
    def emit(self, record: LogRecord) -> None:
        try:
            self._queue.put_nowait(self.format(record))
        except Exception:  # noqa: BLE001
            self.handleError(record)

    async def _process_queue(self) -> None:
        while True:
            messages: list[str] = []
            async with self._lock:
                try:
                    messages.append(self._queue.get_nowait())
                except QueueEmpty:
                    break
                if len(messages) >= 1:
                    _LOGGER.debug("Sending %s messages(s)", len(messages))
                    text = "\n".join(messages)
                    try:
                        await send_to_slack(self._url, text, timeout=self._timeout)
                    except CancelledError:
                        break
                    except Exception:
                        _LOGGER.exception("Slack handler error")

    async def aclose(self) -> None:
        """Close the handler."""
        _ = self._task.cancel()
        with suppress(CancelledError):
            await self._task
        super().close()


async def send_to_slack(
    url: str, text: str, /, *, timeout: Duration = _TIMEOUT
) -> None:
    """Send a message via Slack."""
    client = _get_client(url, timeout=timeout)  # pragma: no cover
    response = await client.send(text=text)  # pragma: no cover
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
    timeout_use = safe_round(duration_to_float(timeout))
    return AsyncWebhookClient(url, timeout=timeout_use)


__all__ = ["SendToSlackError", "SlackHandler", "send_to_slack"]
