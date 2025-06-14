#!/usr/bin/env python
from __future__ import annotations

from logging import getLogger

from click import command

from utilities.logging import basic_config

_LOGGER = getLogger(__name__)


@command()
def main() -> None:
    basic_config(obj=_LOGGER)
    _LOGGER.info("hi")


if __name__ == "__main__":
    main()
