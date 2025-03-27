from __future__ import annotations

from inspect import signature
from pathlib import Path
from random import Random
from time import sleep
from typing import TYPE_CHECKING

from pytest import mark, param, raises

from utilities.pytest import (
    NodeIdToPathError,
    is_pytest,
    node_id_to_path,
    random_state,
    throttle,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from _pytest.legacypath import Testdir  # pyright: ignore[reportPrivateImportUsage]


_ = random_state


class TestIsPytest:
    def test_main(self) -> None:
        assert is_pytest()


class TestNodeIdToPath:
    @mark.parametrize(
        ("node_id", "expected"),
        [
            param(
                "src/tests/module/test_funcs.py::TestClass::test_main",
                Path("src.tests.module.test_funcs", "TestClass__test_main.csv"),
            ),
            param(
                "src/tests/module/test_funcs.py::TestClass::test_main.csv",
                Path("src.tests.module.test_funcs", "TestClass__test_main.csv"),
            ),
            param(
                "src/tests/module/test_funcs.py::TestClass::test_main[param1, param2]",
                Path(
                    "src.tests.module.test_funcs",
                    "TestClass__test_main[param1, param2].csv",
                ),
            ),
            param(
                "src/tests/module/test_funcs.py::TestClass::test_main[EUR.USD]",
                Path(
                    "src.tests.module.test_funcs", "TestClass__test_main[EUR.USD].csv"
                ),
            ),
        ],
    )
    def test_main(self, *, node_id: str, expected: Path) -> None:
        result = node_id_to_path(node_id, suffix=".csv")
        assert result == expected

    def test_head(self) -> None:
        node_id = "src/tests/module/test_funcs.py::TestClass::test_main"
        result = node_id_to_path(node_id, head=Path("src/tests"), suffix=".csv")
        expected = Path("module.test_funcs", "TestClass__test_main.csv")
        assert result == expected

    def test_error_file_suffix(self) -> None:
        with raises(NodeIdToPathError, match="Node ID must be a Python file; got .*"):
            _ = node_id_to_path("src/tests/module/test_funcs.csv::TestClass::test_main")


class TestPytestOptions:
    def test_unknown_mark(self, *, testdir: Testdir) -> None:
        _ = testdir.makepyfile(
            """
            from pytest import mark

            @mark.unknown
            def test_main():
                assert True
            """
        )
        result = testdir.runpytest()
        result.assert_outcomes(errors=1)
        result.stdout.re_match_lines([r".*Unknown pytest\.mark\.unknown"])

    @mark.parametrize("configure", [param(True), param(False)])
    def test_unknown_option(self, *, configure: bool, testdir: Testdir) -> None:
        if configure:
            _ = testdir.makeconftest(
                """
                from utilities.pytest import add_pytest_configure

                def pytest_configure(config):
                    add_pytest_configure(config, [("slow", "slow to run")])
                """
            )
        _ = testdir.makepyfile(
            """
            def test_main():
                assert True
            """
        )
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
        case: Sequence[str],
        passed: int,
        skipped: int,
        matches: Sequence[str],
    ) -> None:
        _ = testdir.makeconftest(
            """
            from utilities.pytest import add_pytest_addoption
            from utilities.pytest import add_pytest_collection_modifyitems
            from utilities.pytest import add_pytest_configure

            def pytest_addoption(parser):
                add_pytest_addoption(parser, ["slow"])

            def pytest_collection_modifyitems(config, items):
                add_pytest_collection_modifyitems(config, items, ["slow"])

            def pytest_configure(config):
                add_pytest_configure(config, [("slow", "slow to run")])
            """
        )
        _ = testdir.makepyfile(
            """
            from pytest import mark

            @mark.slow
            def test_main():
                assert True
            """
        )
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
        case: Sequence[str],
        passed: int,
        skipped: int,
        matches: Sequence[str],
    ) -> None:
        _ = testdir.makeconftest(
            """
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
            """
        )
        _ = testdir.makepyfile(
            """
            from pytest import mark

            def test_none():
                assert True

            @mark.slow
            def test_slow():
                assert True

            @mark.fast
            def test_fast():
                assert True

            @mark.slow
            @mark.fast
            def test_both():
                assert True
            """
        )
        result = testdir.runpytest("-rs", *case, "--randomly-dont-reorganize")
        result.assert_outcomes(passed=passed, skipped=skipped)
        result.stdout.re_match_lines(list(matches))


class TestRandomState:
    def test_main(self, *, random_state: Random) -> None:
        assert isinstance(random_state, Random)


class TestThrottle:
    @mark.parametrize("as_float", [param(True), param(False)])
    @mark.parametrize("on_try", [param(True), param(False)])
    @mark.flaky
    def test_basic(
        self, *, testdir: Testdir, tmp_path: Path, as_float: bool, on_try: bool
    ) -> None:
        root_str = str(tmp_path)
        duration = "1.0" if as_float else "dt.timedelta(seconds=1.0)"
        contents = f"""
            import datetime as dt

            from utilities.pytest import throttle

            @throttle(root={root_str!r}, duration={duration}, on_try={on_try})
            def test_main():
                assert True
            """
        _ = testdir.makepyfile(contents)
        testdir.runpytest().assert_outcomes(passed=1)
        testdir.runpytest().assert_outcomes(skipped=1)
        sleep(1.0)
        testdir.runpytest().assert_outcomes(passed=1)

    @mark.parametrize("asyncio_first", [param(True), param(False)])
    @mark.parametrize("as_float", [param(True), param(False)])
    @mark.parametrize("on_try", [param(True), param(False)])
    @mark.flaky
    def test_async(
        self,
        *,
        testdir: Testdir,
        tmp_path: Path,
        asyncio_first: bool,
        as_float: bool,
        on_try: bool,
    ) -> None:
        root_str = str(tmp_path)
        duration = "1.0" if as_float else "dt.timedelta(seconds=1.0)"
        asyncio_str = "@mark.asyncio"
        throttle_str = (
            f"@throttle(root={root_str!r}, duration={duration}, on_try={on_try})"
        )
        if asyncio_first:
            decorators = f"{asyncio_str}\n{throttle_str}"
        else:
            decorators = f"{throttle_str}\n{asyncio_str}"
        contents = f"""
import datetime as dt

from pytest import mark

from utilities.pytest import throttle

{decorators}
async def test_main():
    assert True
        """
        _ = testdir.makepyfile(contents)
        testdir.runpytest().assert_outcomes(passed=1)
        testdir.runpytest().assert_outcomes(skipped=1)
        sleep(1.0)
        testdir.runpytest().assert_outcomes(passed=1)

    @mark.flaky
    def test_on_pass(self, *, testdir: Testdir, tmp_path: Path) -> None:
        _ = testdir.makeconftest(
            """
            from pytest import fixture

            def pytest_addoption(parser):
                parser.addoption("--pass", action="store_true")

            @fixture
            def is_pass(request):
                return request.config.getoption("--pass")
            """
        )
        root_str = str(tmp_path)
        contents = f"""
            from utilities.pytest import throttle

            @throttle(root={root_str!r}, duration=1.0)
            def test_main(is_pass):
                assert is_pass
            """
        _ = testdir.makepyfile(contents)
        for i in range(2):
            for _ in range(2):
                testdir.runpytest().assert_outcomes(failed=1)
            testdir.runpytest("--pass").assert_outcomes(passed=1)
            for _ in range(2):
                testdir.runpytest("--pass").assert_outcomes(skipped=1)
            if i == 0:
                sleep(1.0)

    @mark.flaky
    def test_on_try(self, *, testdir: Testdir, tmp_path: Path) -> None:
        _ = testdir.makeconftest(
            """
            from pytest import fixture

            def pytest_addoption(parser):
                parser.addoption("--pass", action="store_true")

            @fixture
            def is_pass(request):
                return request.config.getoption("--pass")
            """
        )
        root_str = str(tmp_path)
        contents = f"""
            from utilities.pytest import throttle

            @throttle(root={root_str!r}, duration=1.0, on_try=True)
            def test_main(is_pass):
                assert is_pass
            """
        _ = testdir.makepyfile(contents)
        for i in range(2):
            testdir.runpytest().assert_outcomes(failed=1)
            for _ in range(2):
                testdir.runpytest().assert_outcomes(skipped=1)
            sleep(1.0)
            testdir.runpytest("--pass").assert_outcomes(passed=1)
            for _ in range(2):
                testdir.runpytest().assert_outcomes(skipped=1)
            if i == 0:
                sleep(1.0)

    @mark.flaky
    def test_long_name(self, *, testdir: Testdir, tmp_path: Path) -> None:
        root_str = str(tmp_path)
        contents = f"""
            from pytest import mark

            from string import printable
            from utilities.pytest import throttle

            @mark.parametrize('arg', [10 * printable])
            @throttle(root={root_str!r}, duration=1.0)
            def test_main(*, arg: str):
                assert True
            """
        _ = testdir.makepyfile(contents)
        testdir.runpytest().assert_outcomes(passed=1)
        testdir.runpytest().assert_outcomes(skipped=1)
        sleep(1.0)
        testdir.runpytest().assert_outcomes(passed=1)

    def test_signature(self) -> None:
        @throttle()
        def func(*, fix: bool) -> None:
            assert fix

        def other(*, fix: bool) -> None:
            assert fix

        assert signature(func) == signature(other)
