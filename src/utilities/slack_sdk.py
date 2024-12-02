from __future__ import annotations

from asyncio import CancelledError, Lock, Queue, QueueEmpty, Task, get_event_loop
from dataclasses import dataclass
from http import HTTPStatus
from logging import NOTSET, Handler, LogRecord, getLogger
from typing import TYPE_CHECKING

from slack_sdk.webhook.async_client import AsyncWebhookClient
from typing_extensions import override

from utilities.asyncio import sleep_dur
from utilities.datetime import MINUTE, SECOND, duration_to_float
from utilities.functools import cache
from utilities.math import safe_round
from utilities.timer import Timer

if TYPE_CHECKING:
    from collections.abc import Callable

    from slack_sdk.webhook import WebhookResponse

    from utilities.types import Duration

_LOGGER = getLogger(__name__)
_TIMEOUT = MINUTE
_FREQ = SECOND
_COMPLETE_MULTIPLIER = 5.0


class SlackHandler(Handler):
    """Handler for sending messages to Slack."""

    _url: str
    _timeout: Duration
    _lock: Lock
    _callback: Callable[[], None] | None
    _complete_multiplier: float
    _queue: Queue[str | None]
    _task: Task[None]

    @override
    def __init__(
        self,
        url: str,
        /,
        *,
        level: int = NOTSET,
        timeout: Duration = _TIMEOUT,
        freq: Duration = _FREQ,
        callback: Callable[[], None] | None = None,
        complete_multiplier: float = _COMPLETE_MULTIPLIER,
    ) -> None:
        super().__init__(level=level)
        self._url = url
        self._timeout = timeout
        self._freq = freq
        self._callback = callback
        self._complete_multiplier = complete_multiplier
        self._lock = Lock()
        self._queue = Queue()
        self._task = get_event_loop().create_task(self._process_queue())

    async def complete(self) -> None:
        """Complete the task."""
        async with self._lock:
            await self._queue.put(None)
        max_ = self._complete_multiplier * self._freq
        with Timer() as timer:
            while not self._queue.empty():
                if timer >= max_:  # pragma: no cover
                    raise SlackHandlerError(queue=self._queue)
                await sleep_dur(duration=0.01)

    @override
    def emit(self, record: LogRecord) -> None:
        try:
            self._queue.put_nowait(self.format(record))
        except Exception:  # noqa: BLE001  # pragma: no cover
            self.handleError(record)

    async def _process_queue(self) -> None:
        while True:
            messages: list[str] = []
            exit_ = False
            async with self._lock:
                while True:
                    try:
                        message = self._queue.get_nowait()
                    except QueueEmpty:
                        break
                    else:
                        if message is not None:
                            messages.append(message)
                        exit_ |= message is None
            if len(messages) >= 1:  # pragma: no cover
                _LOGGER.debug("Sending %s messages(s)", len(messages))
                text = "\n".join(messages)
                try:
                    await send_to_slack(self._url, text, timeout=self._timeout)
                except CancelledError:
                    break
                except Exception:
                    _LOGGER.exception("Slack handler error")
                else:
                    if self._callback is not None:
                        self._callback()
            if exit_:
                return
            await sleep_dur(duration=self._freq)


@dataclass(kw_only=True, slots=True)
class SlackHandlerError(Exception):
    queue: Queue[str | None]

    @override
    def __str__(self) -> str:
        return f"Message queue must be empty upon completion; got {self.queue}"  # pragma: no cover


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
    timeout_use = safe_round(duration_to_float(timeout))
    return AsyncWebhookClient(url, timeout=timeout_use)


__all__ = ["SendToSlackError", "SlackHandler", "send_to_slack"]
