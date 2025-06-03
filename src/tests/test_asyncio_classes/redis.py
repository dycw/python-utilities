from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from tests.test_asyncio_classes.loopers import _BACKOFF, _FREQ
from utilities.asyncio import Looper
from utilities.redis import PublishServiceMixin, SubscribeServiceMixin
from utilities.text import unique_str

if TYPE_CHECKING:
    from utilities.types import Duration


@dataclass(kw_only=True)
class LooperWithPublishAndSubscribeMixins(
    PublishServiceMixin[Any], SubscribeServiceMixin[Any], Looper[Any]
):
    freq: Duration = field(default=_FREQ, repr=False)
    backoff: Duration = field(default=_BACKOFF, repr=False)
    _debug: bool = field(default=True, repr=False)
    publish_service_freq: Duration = field(default=_FREQ, repr=False)
    publish_service_backoff: Duration = field(default=_BACKOFF, repr=False)
    publish_service_debug: bool = field(default=True, repr=False)
    subscribe_service_freq: Duration = field(default=_FREQ, repr=False)
    subscribe_service_backoff: Duration = field(default=_BACKOFF, repr=False)
    subscribe_service_debug: bool = field(default=True, repr=False)
    subscribe_service_channel: str = field(default_factory=unique_str)
