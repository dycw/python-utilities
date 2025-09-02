# from __future__ import annotations # noqa: I002

from pathlib import Path
from re import findall
from typing import Self, override

from pytest import mark, param
from typed_settings import FileLoader, TomlFormat, find, load_settings
from typed_settings.converters import get_default_cattrs_converter


@mark.parametrize("text", [param("inner[x=1,y=2]"), param("{x=1, y=2}")])
def test_converter_dataclass(*, tmp_path: Path, text: str) -> None:
    from dataclasses import dataclass

    x, y = 10, 20

    @dataclass(repr=False, frozen=True, kw_only=True, slots=True)
    class Inner:
        x: int
        y: int

        @override
        def __str__(self) -> str:
            return f"inner[{self.x},{self.y}]"

        @classmethod
        def parse(cls, text: str, /) -> Self:
            x, y = findall(r"^inner\[(.+?),(.+?)\]$", text)
            return cls(x=int(x), y=int(y))

    value = Inner(x=x, y=y)

    @dataclass(frozen=True, kw_only=True, slots=True)
    class Settings:
        inner: Inner

        @classmethod
        def parse(cls, value: str | Self, _typ: Self) -> Self:
            if isinstance(value, cls):
                return value

            return cls(...)

    file = tmp_path.joinpath("settings.toml")
    _ = file.write_text(
        f"""
            [app_name]
            inner = {text}
        """
    )
    converter = get_default_cattrs_converter()
    converter.register_structure_hook(Inner, Inner.parse)
    settings = load_settings(
        Settings,
        loaders=[
            FileLoader(
                formats={"*.toml": TomlFormat("app_name")},
                files=[find("pyproject.toml", start_dir=tmp_path)],
            )
        ],
        converter=converter,
        base_dir=tmp_path,
    )
    assert settings.inner == value
