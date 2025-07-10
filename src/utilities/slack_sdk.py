from __future__ import annotations

from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, override

from slack_sdk.webhook import WebhookClient
from slack_sdk.webhook.async_client import AsyncWebhookClient

from utilities.asyncio import timeout_td
from utilities.functools import cache
from utilities.whenever import MINUTE, to_seconds

if TYPE_CHECKING:
    from slack_sdk.webhook import WebhookResponse
    from whenever import TimeDelta

    from utilities.types import Delta


_TIMEOUT: Delta = MINUTE


##


def send_to_slack(url: str, text: str, /, *, timeout: TimeDelta = _TIMEOUT) -> None:
    """Send a message via Slack synchronously."""
    client = _get_client(url, timeout=timeout)
    response = client.send(text=text)
    if response.status_code != HTTPStatus.OK:  # pragma: no cover
        raise SendToSlackError(text=text, response=response)


@cache
def _get_client(url: str, /, *, timeout: Delta = _TIMEOUT) -> WebhookClient:
    """Get the Slack client."""
    return WebhookClient(url, timeout=to_seconds(timeout))


async def send_to_slack_async(
    url: str, text: str, /, *, timeout: TimeDelta = _TIMEOUT
) -> None:
    """Send a message via Slack."""
    client = _get_async_client(url, timeout=timeout)
    async with timeout_td(timeout):
        response = await client.send(text=text)
    if response.status_code != HTTPStatus.OK:  # pragma: no cover
        raise SendToSlackError(text=text, response=response)


@cache
def _get_async_client(
    url: str, /, *, timeout: TimeDelta = _TIMEOUT
) -> AsyncWebhookClient:
    """Get the Slack client."""
    return AsyncWebhookClient(url, timeout=to_seconds(timeout))


@dataclass(kw_only=True, slots=True)
class SendToSlackError(Exception):
    text: str
    response: WebhookResponse

    @override
    def __str__(self) -> str:
        code = self.response.status_code  # pragma: no cover
        phrase = HTTPStatus(code).phrase  # pragma: no cover
        return f"Error sending to Slack:\n\n{self.text}\n\n{code}: {phrase}"  # pragma: no cover


__all__ = ["SendToSlackError", "send_to_slack", "send_to_slack_async"]
