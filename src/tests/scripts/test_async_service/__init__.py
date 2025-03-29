from __future__ import annotations

from logging import getLogger

from utilities.logging import basic_config

_LOGGER = getLogger(__name__)


def main() -> None:
    basic_config()
    _LOGGER.info("hi")
