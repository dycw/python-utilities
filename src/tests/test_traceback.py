from __future__ import annotations

from itertools import chain
from pathlib import Path
from re import search
from subprocess import CalledProcessError, run
from sys import exc_info
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import sampled_from
from pytest import CaptureFixture, mark, param, raises
from pytest_lazy_fixtures import lf

from utilities.constants import SECOND
from utilities.core import get_now, normalize_multi_line_str, one
from utilities.traceback import (
    MakeExceptHookError,
    _make_except_hook_purge,
    _path_to_dots,
    format_exception_stack,
    make_except_hook,
)
from utilities.whenever import format_compact

if TYPE_CHECKING:
    from utilities.types import Delta


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

    @classmethod
    def func_subprocess(cls) -> None:
        _ = run(
            "echo stdout; sleep 0.5; echo stderr 1>&2; exit 1",
            capture_output=True,
            check=True,
            shell=True,
        )

    def test_main(self) -> None:
        try:
            _ = self.func(1, 2, 3, 4, c=5, d=6, e=7)
        except AssertionError as error:
            result = format_exception_stack(error)
            pattern = normalize_multi_line_str(r"""
                ┏━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
                ┃ n=2 ┃                                                                        ┃
                ┡━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
                │ 1   │ tests.test_traceback:\d+:test_main \s+ │
                │     │     _ = self.func\(1, 2, 3, 4, c=5, d=6, e=7\)                           │
                ├─────┼────────────────────────────────────────────────────────────────────────┤
                │ 2   │ tests.test_traceback:\d+:func                                           │
                │     │     assert result % 10 == 0, f"Result \({result}\) must be divisible by  │
                │     │ 10"                                                                    │
                ├─────┼────────────────────────────────────────────────────────────────────────┤
                │ E   │ AssertionError\(Result \(56\) must be divisible by 10                     │
                │     │ assert \(56 % 10\) == 0\)                                                 │
                └─────┴────────────────────────────────────────────────────────────────────────┘
            """)
            self._run_test(result, pattern)

    def test_header(self) -> None:
        try:
            _ = self.func(1, 2, 3, 4, c=5, d=6, e=7)
        except AssertionError as error:
            result = format_exception_stack(error, header=True)
            pattern = normalize_multi_line_str(r"""
                ┌────────────┬─────────────────────────────┐
                │ Date/time  │ \d{8}T\d{6}\[.+\]\s+│
                │ Started    │ \d{8}T\d{6}\[.+\]\s+│
                │ Duration   │ PT\d+\.\d{6}S \s+ │
                │ User       │ \w+ \s+ │
                │ Host       │ [\w\-]+ \s+ │
                │ Process ID │ \d+ \s+ │
                │ Version    │                             │
                └────────────┴─────────────────────────────┘

                ┏━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
                ┃ n=2 ┃                                                                        ┃
                ┡━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
                │ 1   │ tests.test_traceback:\d+:test_header \s+ │
                │     │     _ = self.func\(1, 2, 3, 4, c=5, d=6, e=7\)                           │
                ├─────┼────────────────────────────────────────────────────────────────────────┤
                │ 2   │ tests.test_traceback:\d+:func \s+ │
                │     │     assert result % 10 == 0, f"Result \({result}\) must be divisible by  │
                │     │ 10"                                                                    │
                ├─────┼────────────────────────────────────────────────────────────────────────┤
                │ E   │ AssertionError\(Result \(56\) must be divisible by 10                     │
                │     │ assert \(56 % 10\) == 0\)                                                 │
                └─────┴────────────────────────────────────────────────────────────────────────┘
            """)
            self._run_test(result, pattern)

    def test_capture_locals(self) -> None:
        try:
            _ = self.func(1, 2, 3, 4, c=5, d=6, e=7)
        except AssertionError as error:
            result = format_exception_stack(error, capture_locals=True)
            pattern = normalize_multi_line_str(r"""
                ┏━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
                ┃ n=2 ┃                                                                        ┃
                ┡━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
                │ 1   │ tests.test_traceback:\d+:test_capture_locals \s+ │
                │     │     _ = self.func\(1, 2, 3, 4, c=5, d=6, e=7\)                           │
                ├─────┼────────────────────────────────────────────────────────────────────────┤
                │     │ ┌───────┬────────────────────────────────────────────────────────────┐ │
                │     │ │ self  │ <tests.test_traceback.TestFormatExceptionStack object at   │ │
                │     │ │       │ 0x[0-9a-z]{9}>                                               │ │
                │     │ │ error │ AssertionError\('Result \(56\) must be divisible by           │ │
                │     │ │       │ 10\\nassert \(56 % 10\) == 0'\)                                │ │
                │     │ └───────┴────────────────────────────────────────────────────────────┘ │
                ├─────┼────────────────────────────────────────────────────────────────────────┤
                │ 2   │ tests.test_traceback:\d+:func \s+ │
                │     │     assert result % 10 == 0, f"Result \({result}\) must be divisible by  │
                │     │ 10"                                                                    │
                ├─────┼────────────────────────────────────────────────────────────────────────┤
                │     │ ┌─────────────┬──────────────────────────────────────────────────────┐ │
                │     │ │ cls         │ <class                                               │ │
                │     │ │             │ 'tests.test_traceback.TestFormatExceptionStack'>     │ │
                │     │ │ a           │ 2                                                    │ │
                │     │ │ b           │ 4                                                    │ │
                │     │ │ c           │ 10                                                   │ │
                │     │ │ args        │ \(6, 8\)                                               │ │
                │     │ │ kwargs      │ {'d': 12, 'e': 14}                                   │ │
                │     │ │ result      │ 56                                                   │ │
                │     │ │ @py_assert1 │ 10                                                   │ │
                │     │ │ @py_assert3 │ 6                                                    │ │
                │     │ │ @py_assert5 │ 0                                                    │ │
                │     │ │ @py_assert4 │ False                                                │ │
                │     │ │ @py_format7 │ '\(56 % 10\) == 0'                                     │ │
                │     │ │ @py_format9 │ 'Result \(56\) must be divisible by 10\\n>assert \(56 %  │ │
                │     │ │             │ 10\) == 0'                                            │ │
                │     │ └─────────────┴──────────────────────────────────────────────────────┘ │
                ├─────┼────────────────────────────────────────────────────────────────────────┤
                │ E   │ AssertionError\(Result \(56\) must be divisible by 10                     │
                │     │ assert \(56 % 10\) == 0\)                                                 │
                └─────┴────────────────────────────────────────────────────────────────────────┘
            """)
            self._run_test(result, pattern)

    def test_subprocess(self) -> None:
        try:
            _ = self.func_subprocess()
        except CalledProcessError as error:
            result = format_exception_stack(error)
            pattern = normalize_multi_line_str(r"""
                ┏━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
                ┃ n=3 ┃                                                                   ┃
                ┡━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
                │ 1   │ tests.test_traceback:\d+:test_subprocess \s+ │
                │     │     _ = self.func_subprocess\(\)                                    │
                ├─────┼───────────────────────────────────────────────────────────────────┤
                │ 2   │ tests.test_traceback:\d+:func_subprocess \s+ │
                │     │     _ = run\(                                                      │
                ├─────┼───────────────────────────────────────────────────────────────────┤
                │ 3   │ subprocess:\d+:run \s+ │
                │     │     raise CalledProcessError\(retcode, process.args,               │
                ├─────┼───────────────────────────────────────────────────────────────────┤
                │ E   │ CalledProcessError\(                                               │
                │     │     returncode │ 1                                                │
                │     │     cmd        │ echo stdout; sleep 0.5; echo stderr 1>&2; exit 1 │
                │     │     stdout     │ b'stdout\\n'                                      │
                │     │     stderr     │ b'stderr\\n'                                      │
                │     │ \)                                                                 │
                └─────┴───────────────────────────────────────────────────────────────────┘
            """)
            self._run_test(result, pattern)

    def _run_test(self, result: str, pattern: str, /) -> None:
        result_lines, pattern_lines = [t.splitlines() for t in [result, pattern]]
        m, n = [len(lines) for lines in [result_lines, pattern_lines]]
        assert m == n
        for i in range(1, m + 1):
            result_i = "\n".join(result_lines[:i])
            pattern_i = "\n".join(pattern_lines[:i])
            assert search(pattern_i, result_i) is not None, f"""\
Failure up to line {i}/{m}:

---- RESULT -------------------------------------------------------------------
{result}

---- PATTERN ------------------------------------------------------------------
{pattern}"""


class TestMakeExceptHook:
    def test_main(self, *, capsys: CaptureFixture) -> None:
        hook = make_except_hook()
        try:
            _ = 1 / 0
        except ZeroDivisionError:
            exc_type, exc_val, traceback = exc_info()
            hook(exc_type, exc_val, traceback)
            assert capsys.readouterr() != ""

    @mark.parametrize("path_max_age", [param(SECOND), param(None)])
    def test_path(self, *, tmp_path: Path, path_max_age: Delta | None) -> None:
        hook = make_except_hook(path=tmp_path, path_max_age=path_max_age)
        try:
            _ = 1 / 0
        except ZeroDivisionError:
            exc_type, exc_val, traceback = exc_info()
            hook(exc_type, exc_val, traceback)
        path = one(tmp_path.iterdir())
        assert search(r"^.+?\.txt$", path.name)

    def test_non_error(self) -> None:
        hook = make_except_hook()
        exc_type, exc_val, traceback = exc_info()
        with raises(MakeExceptHookError, match=r"No exception to log"):
            hook(exc_type, exc_val, traceback)


class TestMakeExceptHookPurge:
    def test_main(self, *, tmp_path: Path) -> None:
        now = get_now()
        path = tmp_path.joinpath(
            format_compact(now - 2 * SECOND, path=True)
        ).with_suffix(".txt")
        path.touch()
        assert len(list(tmp_path.iterdir())) == 1
        _make_except_hook_purge(tmp_path, SECOND)
        assert len(list(tmp_path.iterdir())) == 0

    @mark.parametrize(
        "path", [param(lf("temp_dir_with_dir")), param(lf("temp_dir_with_file"))]
    )
    def test_purge_invalid_path(self, *, path: Path) -> None:
        _make_except_hook_purge(path, SECOND)
        assert len(list(path.iterdir())) == 1


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
