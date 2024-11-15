from __future__ import annotations

from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING

from slack_sdk.webhook import WebhookClient, WebhookResponse
from slack_sdk.webhook.async_client import AsyncWebhookClient
from typing_extensions import override

from utilities.datetime import MINUTE, duration_to_float
from utilities.functools import cache
from utilities.math import safe_round

if TYPE_CHECKING:
    from utilities.types import Duration

_TIMEOUT = MINUTE


def send_slack_sync(text: str, /, *, url: str, timeout: Duration = _TIMEOUT) -> None:
    """Send a message to Slack, synchronously."""
    client = _get_client_sync(url, timeout=timeout)  # pragma: no cover
    response = client.send(text=text)  # pragma: no cover
    _check_status_code(text, response)  # pragma: no cover


async def send_slack_async(
    text: str, /, *, url: str, timeout: Duration = _TIMEOUT
) -> None:
    """Send a message via Slack."""
    client = _get_client_async(url, timeout=timeout)  # pragma: no cover
    response = await client.send(text=text)  # pragma: no cover
    _check_status_code(text, response)  # pragma: no cover


def _check_status_code(text: str, response: WebhookResponse, /) -> None:
    """Check that a chunk was successfully sent."""
    if response.status_code != HTTPStatus.OK:  # pragma: no cover
        raise SendSlackError(text=text, response=response)


@dataclass(kw_only=True, slots=True)
class SendSlackError(Exception):
    text: str
    response: WebhookResponse

    @override
    def __str__(self) -> str:
        code = self.response.status_code  # pragma: no cover
        phrase = HTTPStatus(code).phrase  # pragma: no cover
        return f"Error sending to Slack:\n\n{self.text}\n\n{code}: {phrase}"  # pragma: no cover


@cache
def _get_client_sync(url: str, /, *, timeout: Duration = _TIMEOUT) -> WebhookClient:
    """Get the webhook client."""
    timeout_use = safe_round(duration_to_float(timeout))
    return WebhookClient(url, timeout=timeout_use)


@cache
def _get_client_async(
    url: str, /, *, timeout: Duration = _TIMEOUT
) -> AsyncWebhookClient:
    """Get the engine/sessionmaker for the required database."""
    timeout_use = safe_round(duration_to_float(timeout))
    return AsyncWebhookClient(url, timeout=timeout_use)


__all__ = ["send_slack_async", "send_slack_sync"]
