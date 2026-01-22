from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pytest import fixture, mark, param, raises

from utilities.constants import IS_CI, SECOND
from utilities.core import repr_str
from utilities.functions import in_seconds
from utilities.pytest import (
    _NodeIdToPathNotGetTailError,
    _NodeIdToPathNotPythonFileError,
    node_id_path,
)
from utilities.time import sleep

if TYPE_CHECKING:
    from _pytest.legacypath import Testdir
    from whenever import TimeDelta


_DURATION: TimeDelta = (5 if IS_CI else 1) * SECOND
_MULTIPLE: float = 2


@fixture(autouse=True)
def set_asyncio_default_fixture_loop_scope(*, testdir: Testdir) -> None:
    _ = testdir.makepyprojecttoml("""
        [tool.pytest]
        asyncio_default_fixture_loop_scope = "function"
    """)


class TestMakeIDs:
    def test_main(self, *, testdir: Testdir) -> None:
        _ = testdir.makepyfile("""
            from pytest import mark, param

            from utilities.pytest import make_ids

            @mark.parametrize('n', [param(1), param(2), param(3)], ids=make_ids)
            def test_main(*, n: int) -> None:
                assert isinstance(n, int)
        """)
        testdir.runpytest("-p", "xdist", "-n", "2").assert_outcomes(passed=3)

    def test_functions(self, *, testdir: Testdir) -> None:
        _ = testdir.makepyfile("""
            from collections.abc import Callable

            from pytest import mark, param

            from utilities.pytest import make_ids

            def f() -> int:
                return 1

            def g() -> int:
                return 2

            def h() -> int:
                return 3

            @mark.parametrize('func', [param(f), param(g), param(h)], ids=make_ids)
            def test_main(*, func: Callable[[], int]) -> None:
                assert isinstance(func(), int)
        """)
        testdir.runpytest("-p", "xdist", "-n", "2").assert_outcomes(passed=3)

    def test_sqlalchemy(self, *, testdir: Testdir) -> None:
        _ = testdir.makepyfile("""
            from typing import Any

            from pytest import mark, param
            from sqlalchemy import INTEGER, Integer

            from utilities.pytest import make_ids

            @mark.parametrize('obj', [param(INTEGER), param(Integer)], ids=make_ids)
            def test_main(*, obj: Any) -> None:
                assert isinstance(obj, object)
        """)
        testdir.runpytest("-p", "xdist", "-n", "2").assert_outcomes(passed=2)


class TestNodeIdPath:
    @mark.parametrize(
        ("node_id", "expected"),
        [
            param(
                "src/tests/module/test_funcs.py::TestClass::test_main",
                Path("src.tests.module.test_funcs/TestClass__test_main"),
            ),
            param(
                "src/tests/module/test_funcs.py::TestClass::test_main[param1, param2]",
                Path(
                    "src.tests.module.test_funcs/TestClass__test_main[param1, param2]"
                ),
            ),
            param(
                "src/tests/module/test_funcs.py::TestClass::test_main[EUR.USD]",
                Path("src.tests.module.test_funcs/TestClass__test_main[EUR.USD]"),
            ),
        ],
    )
    def test_main(self, *, node_id: str, expected: Path) -> None:
        result = node_id_path(node_id)
        assert result == expected

    @mark.parametrize(
        "node_id",
        [
            param("src/tests/module/test_funcs.py::TestClass::test_main"),
            param("tests/module/test_funcs.py::TestClass::test_main"),
            param(
                "python/package/src/tests/module/test_funcs.py::TestClass::test_main"
            ),
        ],
    )
    def test_root(self, *, node_id: str) -> None:
        result = node_id_path(node_id, root="tests")
        expected = Path("module.test_funcs/TestClass__test_main")
        assert result == expected

    @mark.parametrize(
        "node_id",
        [
            param("src/tests/module/test_funcs.py::TestClass::test_main"),
            param("tests/module/test_funcs.py::TestClass::test_main"),
        ],
    )
    def test_suffix(self, *, node_id: str) -> None:
        result = node_id_path(node_id, root="tests", suffix=".csv")
        expected = Path("module.test_funcs/TestClass__test_main.csv")
        assert result == expected

    def test_error_not_python_file(self) -> None:
        with raises(
            _NodeIdToPathNotPythonFileError,
            match=r"Node ID must be a Python file; got .*",
        ):
            _ = node_id_path("src/tests/module/test_funcs.csv::TestClass::test_main")

    def test_error_get_tail_error(self) -> None:
        with raises(
            _NodeIdToPathNotGetTailError,
            match=r"Unable to get the tail of 'tests.+module.+test_funcs' with root 'src.+tests'",
        ):
            _ = node_id_path(
                "tests/module/test_funcs.py::TestClass::test_main",
                root=Path("src", "tests"),
            )


class TestPytestOptions:
    def test_unknown_mark(self, *, testdir: Testdir) -> None:
        _ = testdir.makepyfile("""
            from pytest import mark

            @mark.unknown
            def test_main() -> None:
                assert True
        """)
        result = testdir.runpytest()
        result.assert_outcomes(errors=1)
        result.stdout.re_match_lines([r".*Unknown pytest\.mark\.unknown"])

    @mark.parametrize("configure", [param(True), param(False)])
    def test_unknown_option(self, *, configure: bool, testdir: Testdir) -> None:
        if configure:
            _ = testdir.makeconftest("""
                from utilities.pytest import add_pytest_configure

                def pytest_configure(config):
                    add_pytest_configure(config, [("slow", "slow to run")])
            """)
        _ = testdir.makepyfile("""
            def test_main() -> None:
                assert True
        """)
        result = testdir.runpytest("--unknown")
        result.stderr.re_match_lines([r".*unrecognized arguments.*"])

    @mark.parametrize(
        ("case", "passed", "skipped", "matches"),
        [param([], 0, 1, [".*3: pass --slow"]), param(["--slow"], 1, 0, [])],
    )
    def test_one_mark_and_option(
        self,
        *,
        testdir: Testdir,
        case: list[str],
        passed: int,
        skipped: int,
        matches: list[str],
    ) -> None:
        _ = testdir.makeconftest("""
            from utilities.pytest import add_pytest_addoption
            from utilities.pytest import add_pytest_collection_modifyitems
            from utilities.pytest import add_pytest_configure

            def pytest_addoption(parser):
                add_pytest_addoption(parser, ["slow"])

            def pytest_collection_modifyitems(config, items):
                add_pytest_collection_modifyitems(config, items, ["slow"])

            def pytest_configure(config):
                add_pytest_configure(config, [("slow", "slow to run")])
        """)
        _ = testdir.makepyfile("""
            from pytest import mark

            @mark.slow
            def test_main() -> None:
                assert True
        """)
        result = testdir.runpytest("-rs", *case)
        result.assert_outcomes(passed=passed, skipped=skipped)
        result.stdout.re_match_lines(list(matches))

    @mark.parametrize(
        ("case", "passed", "skipped", "matches"),
        [
            param(
                [],
                1,
                3,
                [
                    "SKIPPED.*: pass --slow",
                    "SKIPPED.*: pass --fast",
                    "SKIPPED.*: pass --slow --fast",
                ],
            ),
            param(
                ["--slow"],
                2,
                2,
                ["SKIPPED.*: pass --fast", "SKIPPED.*: pass --slow --fast"],
            ),
            param(
                ["--fast"],
                2,
                2,
                ["SKIPPED.*: pass --slow", "SKIPPED.*: pass --slow --fast"],
            ),
            param(["--slow", "--fast"], 4, 0, []),
        ],
    )
    def test_two_marks_and_options(
        self,
        *,
        testdir: Testdir,
        case: list[str],
        passed: int,
        skipped: int,
        matches: list[str],
    ) -> None:
        _ = testdir.makeconftest("""
            from utilities.pytest import add_pytest_addoption
            from utilities.pytest import add_pytest_collection_modifyitems
            from utilities.pytest import add_pytest_configure

            def pytest_addoption(parser):
                add_pytest_addoption(parser, ["slow", "fast"])

            def pytest_collection_modifyitems(config, items):
                add_pytest_collection_modifyitems(
                    config, items, ["slow", "fast"],
                )

            def pytest_configure(config):
                add_pytest_configure(
                    config, [("slow", "slow to run"), ("fast", "fast to run")],
                )
        """)
        _ = testdir.makepyfile("""
            from pytest import mark

            def test_none() -> None:
                assert True

            @mark.slow
            def test_slow() -> None:
                assert True

            @mark.fast
            def test_fast() -> None:
                assert True

            @mark.slow
            @mark.fast
            def test_both() -> None:
                assert True
        """)
        result = testdir.runpytest("-rs", *case, "--randomly-dont-reorganize")
        result.assert_outcomes(passed=passed, skipped=skipped)
        result.stdout.re_match_lines(list(matches))


class TestRunTestFrac:
    def test_sync_func_passing(self, *, testdir: Testdir) -> None:
        _ = testdir.makepyfile("""
            from utilities.pytest import run_test_frac

            @run_test_frac(frac=1.0)
            def test_main() -> None:
                assert True
        """)
        testdir.runpytest().assert_outcomes(passed=1)

    def test_sync_func_skipped(self, *, testdir: Testdir) -> None:
        _ = testdir.makepyfile("""
            from utilities.pytest import run_test_frac

            @run_test_frac(frac=0.0)
            def test_main() -> None:
                assert True
        """)
        testdir.runpytest().assert_outcomes(skipped=1)

    def test_async_func_passing(self, *, testdir: Testdir) -> None:
        _ = testdir.makepyfile("""
            from pytest import mark

            from utilities.pytest import run_test_frac

            @mark.asyncio
            @run_test_frac(frac=1.0)
            async def test_main() -> None:
                assert True
        """)
        testdir.runpytest().assert_outcomes(passed=1)

    def test_async_func_skipped(self, *, testdir: Testdir) -> None:
        _ = testdir.makepyfile("""
            from pytest import mark

            from utilities.pytest import run_test_frac

            @mark.asyncio
            @run_test_frac(frac=0.0)
            async def test_main() -> None:
                assert True
        """)
        testdir.runpytest().assert_outcomes(skipped=1)


class TestThrottleTest:
    def test_main(self, *, testdir: Testdir, tmp_path: Path) -> None:
        seconds = in_seconds(_DURATION)
        _ = testdir.makepyfile(f"""
            from utilities.pytest import throttle_test

            @throttle_test(root={repr_str(tmp_path)}, duration={seconds})
            def test_main() -> None:
                assert True
        """)
        testdir.runpytest().assert_outcomes(passed=1)
        testdir.runpytest().assert_outcomes(skipped=1)
        sleep(_MULTIPLE * _DURATION)
        testdir.runpytest().assert_outcomes(passed=1)

    def test_long_name(self, *, testdir: Testdir, tmp_path: Path) -> None:
        seconds = in_seconds(_DURATION)
        _ = testdir.makepyfile(f"""
            from pytest import mark
            from string import printable

            from utilities.pytest import throttle_test

            @mark.parametrize("arg", [10 * printable])
            @throttle_test(root={str(tmp_path)!r}, duration={seconds})
            def test_main(*, arg: str) -> None:
                assert True
        """)
        testdir.runpytest().assert_outcomes(passed=1)
        testdir.runpytest().assert_outcomes(skipped=1)
        sleep(_MULTIPLE * _DURATION)
        testdir.runpytest().assert_outcomes(passed=1)
