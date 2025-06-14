#!/usr/bin/env python
from __future__ import annotations

from logging import getLogger
from tomllib import loads

from click import command

from utilities.git import get_repo_root
from utilities.logging import basic_config

_LOGGER = getLogger(__name__)


@command()
def main() -> None:
    basic_config(obj=_LOGGER)
    dict_ = loads(get_repo_root().joinpath("pyproject.toml").read_text())
    _LOGGER.info(dict_)


if __name__ == "__main__":
    main()
