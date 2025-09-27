from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from utilities.pydantic_settings import BaseSettings, PathLikeOrWithSection

if TYPE_CHECKING:
    from collections.abc import Sequence


class Foo(BaseSettings):
    toml_files: ClassVar[Sequence[PathLikeOrWithSection]] = [
        Path(__file__).parent.joinpath("config.toml")
    ]

    a: int
