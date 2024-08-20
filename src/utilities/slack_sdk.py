from __future__ import annotations

from dataclasses import dataclass
from http import HTTPStatus

from slack_sdk.webhook import WebhookClient, WebhookResponse
from slack_sdk.webhook.async_client import AsyncWebhookClient
from typing_extensions import override

from utilities.functools import cache

_TIMEOUT = 30


def send_slack_sync(text: str, /, *, url: str, timeout: int = _TIMEOUT) -> None:
    """Send a message to Slack, synchronously."""
    client = _get_client_sync(url, timeout=timeout)  # pragma: no cover
    response = client.send(text=text)  # pragma: no cover
    _check_status_code(response)  # pragma: no cover


async def send_slack_async(
    text: str,
    /,
    *,
    url: str,
    timeout: int = _TIMEOUT,  # noqa: ASYNC109
) -> None:
    """Send a message via Slack."""
    client = _get_client_async(url, timeout=timeout)  # pragma: no cover
    response = await client.send(text=text)  # pragma: no cover
    _check_status_code(response)  # pragma: no cover


def _check_status_code(response: WebhookResponse, /) -> None:
    """Check that a chunk was successfully sent."""
    if response.status_code != HTTPStatus.OK:  # pragma: no cover
        raise SendSlackError(response=response)


@dataclass(kw_only=True)
class SendSlackError(Exception):
    response: WebhookResponse

    @override
    def __str__(self) -> str:
        return f"Webhook response was not OK; got {self.response.status_code}"  # pragma: no cover


@cache
def _get_client_sync(url: str, /, *, timeout: int = _TIMEOUT) -> WebhookClient:
    """Get the webhook client."""
    return WebhookClient(url, timeout=timeout)


@cache
def _get_client_async(url: str, /, *, timeout: int = _TIMEOUT) -> AsyncWebhookClient:
    """Get the engine/sessionmaker for the required database."""
    return AsyncWebhookClient(url, timeout=timeout)


__all__ = ["send_slack_async", "send_slack_sync"]
