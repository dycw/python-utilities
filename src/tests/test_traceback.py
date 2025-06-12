from __future__ import annotations

from itertools import chain
from pathlib import Path
from re import search
from sys import exc_info
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import sampled_from
from pytest import CaptureFixture, raises

from utilities.iterables import one
from utilities.traceback import (
    MakeExceptHookError,
    _path_to_dots,
    format_exception_stack,
    make_except_hook,
)

if TYPE_CHECKING:
    from collections.abc import Iterable


class TestFormatExceptionStack:
    @classmethod
    def func(cls, a: int, b: int, /, *args: int, c: int = 0, **kwargs: int) -> int:
        a *= 2
        b *= 2
        args = tuple(2 * arg for arg in args)
        c *= 2
        kwargs = {k: 2 * v for k, v in kwargs.items()}
        result = sum(chain([a, b], args, [c], kwargs.values()))
        assert result % 10 == 0, f"Result ({result}) must be divisible by 10"
        return result

    def test_main(self) -> None:
        try:
            _ = self.func(1, 2, 3, 4, c=5, d=6, e=7)
        except AssertionError as error:
            result = format_exception_stack(error).splitlines()
            self._assert_lines(result)

    def test_header(self) -> None:
        try:
            _ = self.func(1, 2, 3, 4, c=5, d=6, e=7)
        except AssertionError as error:
            result = format_exception_stack(error, header=True).splitlines()
            patterns = [
                r"^Date/time \| .+$",
                r"^Started   \| .+$",
                r"^Duration  \| .+$",
                r"^User      \| .+$",
                r"^Host      \| .+$",
                r"^Version   \|\s$",
                r"^$",
            ]
            for line, pattern in zip(result[:7], patterns[:7], strict=False):
                assert search(pattern, line), line
            self._assert_lines(result[7:])

    def test_capture_locals(self) -> None:
        try:
            _ = self.func(1, 2, 3, 4, c=5, d=6, e=7)
        except AssertionError as error:
            result = format_exception_stack(error, capture_locals=True).splitlines()
            assert len(result) == 19
            indices = [0, 3, 17, 18]
            self._assert_lines([result[i] for i in indices])
            for i in set(range(len(result))) - set(indices):
                assert search(r"^    \| .+ = .+$", result[i])

    def _assert_lines(self, lines: Iterable[str], /) -> None:
        expected = [
            r"^1/2 \| tests\.test_traceback:\d+ \| test_\w+ \| _ = self\.func\(1, 2, 3, 4, c=5, d=6, e=7\)$",
            r'^2/2 \| tests\.test_traceback:\d+ \| func \| assert result % 10 == 0, f"Result \({result}\) must be divisible by 10"$',
            r"^AssertionError\(Result \(56\) must be divisible by 10$",
            r"^assert \(56 % 10\) == 0\)$",
        ]
        for line, pattern in zip(lines, expected, strict=True):
            assert search(pattern, line), line


class TestMakeExceptHook:
    def test_main(self, *, capsys: CaptureFixture) -> None:
        hook = make_except_hook()
        try:
            _ = 1 / 0
        except ZeroDivisionError:
            exc_type, exc_val, traceback = exc_info()
            hook(exc_type, exc_val, traceback)
            assert capsys.readouterr() != ""

    def test_file(self, *, tmp_path: Path) -> None:
        hook = make_except_hook(path=tmp_path)
        try:
            _ = 1 / 0
        except ZeroDivisionError:
            exc_type, exc_val, traceback = exc_info()
            hook(exc_type, exc_val, traceback)
        path = one(tmp_path.iterdir())
        assert search(r"^[\dT]+\.txt$", path.name)

    def test_non_error(self) -> None:
        hook = make_except_hook()
        exc_type, exc_val, traceback = exc_info()
        with raises(MakeExceptHookError, match="No exception to log"):
            hook(exc_type, exc_val, traceback)


class TestPathToDots:
    @given(
        case=sampled_from([
            (
                Path("repo", ".venv", "lib", "site-packages", "click", "core.py"),
                "click.core",
            ),
            (
                Path(
                    "repo", ".venv", "lib", "site-packages", "utilities", "traceback.py"
                ),
                "utilities.traceback",
            ),
            (Path("repo", ".venv", "bin", "cli.py"), "bin.cli"),
            (Path("src", "utilities", "foo", "bar.py"), "utilities.foo.bar"),
            (
                Path(
                    "uv",
                    "python",
                    "cpython-3.13.0-macos-aarch64-none",
                    "lib",
                    "python3.13",
                    "asyncio",
                    "runners.py",
                ),
                "asyncio.runners",
            ),
            (Path("unknown", "file.py"), "unknown.file"),
        ])
    )
    def test_main(self, *, case: tuple[Path, str]) -> None:
        path, expected = case
        result = _path_to_dots(path)
        assert result == expected
