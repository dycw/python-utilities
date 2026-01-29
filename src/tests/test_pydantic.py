# ruff: noqa: TC001
from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, SecretStr
from pytest import mark, param

from utilities.constants import HOME
from utilities.pydantic import ExpandedPath, extract_secret
from utilities.types import PathLike, SecretLike


class TestExpandedPath:
    @mark.parametrize("path", [param(Path("~")), param("~")])
    def test_main(self, *, path: PathLike) -> None:
        class Example(BaseModel):
            path: ExpandedPath

        _ = Example.model_rebuild()

        result = Example(path=path).path
        assert result == HOME


class TestExtractSecret:
    @mark.parametrize("value", [param(SecretStr("x")), param("x")])
    def test_main(self, *, value: SecretLike) -> None:
        result = extract_secret(value)
        assert result == "x"
