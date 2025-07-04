from __future__ import annotations

from inspect import signature
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING, ClassVar

from pytest import fixture, mark, param, raises

from utilities.iterables import one
from utilities.os import temp_environ
from utilities.pytest import NodeIdToPathError, is_pytest, node_id_path, throttle

if TYPE_CHECKING:
    from collections.abc import Sequence

    from _pytest.legacypath import Testdir


@fixture(autouse=True)
def set_asyncio_default_fixture_loop_scope(*, testdir: Testdir) -> None:
    _ = testdir.makepyprojecttoml("""
        [tool.pytest.ini_options]
        asyncio_default_fixture_loop_scope = "function"
    """)


class TestIsPytest:
    def test_main(self) -> None:
        assert is_pytest()


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
            param(
                "python/package/src/tests/module/test_funcs.py::TestClass::test_main"
            ),
        ],
    )
    def test_root(self, *, node_id: str) -> None:
        result = node_id_path(node_id, root="src/tests")
        expected = Path("module.test_funcs/TestClass__test_main")
        assert result == expected

    def test_suffix(self) -> None:
        node_id = "src/tests/module/test_funcs.py::TestClass::test_main"
        result = node_id_path(node_id, root="src/tests", suffix=".csv")
        expected = Path("module.test_funcs/TestClass__test_main.csv")
        assert result == expected

    def test_error_file_suffix(self) -> None:
        with raises(NodeIdToPathError, match="Node ID must be a Python file; got .*"):
            _ = node_id_path("src/tests/module/test_funcs.csv::TestClass::test_main")


class TestPytestOptions:
    def test_unknown_mark(self, *, testdir: Testdir) -> None:
        _ = testdir.makepyfile(
            """
            from pytest import mark

            @mark.unknown
            def test_main() -> None:
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
            def test_main() -> None:
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
            def test_main() -> None:
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
            """
        )
        result = testdir.runpytest("-rs", *case, "--randomly-dont-reorganize")
        result.assert_outcomes(passed=passed, skipped=skipped)
        result.stdout.re_match_lines(list(matches))


class TestThrottle:
    delta: ClassVar[float] = 0.5

    @mark.flaky
    @mark.parametrize("on_try", [param(True), param(False)])
    def test_basic(self, *, testdir: Testdir, tmp_path: Path, on_try: bool) -> None:
        _ = testdir.makepyfile(
            f"""
            from whenever import TimeDelta

            from utilities.pytest import throttle

            @throttle(root={str(tmp_path)!r}, delta=TimeDelta(seconds={self.delta}), on_try={on_try})
            def test_main() -> None:
                assert True
            """
        )
        testdir.runpytest().assert_outcomes(passed=1)
        testdir.runpytest().assert_outcomes(skipped=1)
        sleep(self.delta)
        testdir.runpytest().assert_outcomes(passed=1)

    @mark.flaky
    @mark.parametrize("asyncio_first", [param(True), param(False)])
    @mark.parametrize("on_try", [param(True), param(False)])
    def test_async(
        self, *, testdir: Testdir, tmp_path: Path, asyncio_first: bool, on_try: bool
    ) -> None:
        if asyncio_first:
            _ = testdir.makepyfile(
                f"""
                from whenever import TimeDelta

                from pytest import mark

                from utilities.pytest import throttle

                @mark.asyncio
                @throttle(root={str(tmp_path)!r}, delta=TimeDelta(seconds={self.delta}), on_try={on_try})
                async def test_main() -> None:
                    assert True
                """
            )
        else:
            _ = testdir.makepyfile(
                f"""
                from whenever import TimeDelta

                from pytest import mark

                from utilities.pytest import throttle

                @throttle(root={str(tmp_path)!r}, delta=TimeDelta(seconds={self.delta}), on_try={on_try})
                @mark.asyncio
                async def test_main() -> None:
                    assert True
                """
            )
        testdir.runpytest().assert_outcomes(passed=1)
        testdir.runpytest().assert_outcomes(skipped=1)
        sleep(self.delta)
        testdir.runpytest().assert_outcomes(passed=1)

    @mark.flaky
    @mark.parametrize("on_try", [param(True), param(False)])
    def test_disabled_via_env_var(
        self, *, testdir: Testdir, tmp_path: Path, on_try: bool
    ) -> None:
        _ = testdir.makepyfile(
            f"""
            from whenever import TimeDelta

            from utilities.pytest import throttle

            @throttle(root={str(tmp_path)!r}, delta=TimeDelta(seconds={self.delta}), on_try={on_try})
            def test_main() -> None:
                assert True
            """
        )
        with temp_environ(THROTTLE="1"):
            testdir.runpytest().assert_outcomes(passed=1)
            testdir.runpytest().assert_outcomes(passed=1)
            sleep(self.delta)
            testdir.runpytest().assert_outcomes(passed=1)

    @mark.flaky
    def test_on_pass(self, *, testdir: Testdir, tmp_path: Path) -> None:
        _ = testdir.makeconftest(
            """
            from pytest import fixture

            def pytest_addoption(parser):
                parser.addoption("--pass", action="store_true")

            @fixture
            def is_pass(request) -> bool:
                return request.config.getoption("--pass")
            """
        )
        _ = testdir.makepyfile(
            f"""
            from whenever import TimeDelta

            from utilities.pytest import throttle

            @throttle(root={str(tmp_path)!r}, delta=TimeDelta(seconds={self.delta}))
            def test_main(*, is_pass: bool) -> None:
                assert is_pass
            """
        )
        for delta_use in [self.delta, 0.0]:
            testdir.runpytest().assert_outcomes(failed=1)
            testdir.runpytest("--pass").assert_outcomes(passed=1)
            testdir.runpytest("--pass").assert_outcomes(skipped=1)
            sleep(delta_use)

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
        _ = testdir.makepyfile(
            f"""
            from whenever import TimeDelta

            from utilities.pytest import throttle

            @throttle(root={root_str!r}, delta=TimeDelta(seconds={self.delta}), on_try=True)
            def test_main(*, is_pass: bool) -> None:
                assert is_pass
            """
        )
        for delta_use in [self.delta, 0.0]:
            testdir.runpytest().assert_outcomes(failed=1)
            testdir.runpytest().assert_outcomes(skipped=1)
            sleep(self.delta)
            testdir.runpytest("--pass").assert_outcomes(passed=1)
            testdir.runpytest().assert_outcomes(skipped=1)
            sleep(delta_use)

    @mark.flaky
    def test_long_name(self, *, testdir: Testdir, tmp_path: Path) -> None:
        _ = testdir.makepyfile(
            f"""
            from pytest import mark
            from string import printable
            from whenever import TimeDelta

            from utilities.pytest import throttle

            @mark.parametrize("arg", [10 * printable])
            @throttle(root={str(tmp_path)!r}, delta=TimeDelta(seconds={self.delta}))
            def test_main(*, arg: str) -> None:
                assert True
            """
        )
        testdir.runpytest().assert_outcomes(passed=1)
        testdir.runpytest().assert_outcomes(skipped=1)
        sleep(self.delta)
        testdir.runpytest().assert_outcomes(passed=1)

    def test_signature(self) -> None:
        @throttle()
        def func(*, fix: bool) -> None:
            assert fix

        def other(*, fix: bool) -> None:
            assert fix

        assert signature(func) == signature(other)

    @mark.flaky
    def test_error_decoding_timestamp(
        self, *, testdir: Testdir, tmp_path: Path
    ) -> None:
        _ = testdir.makepyfile(
            f"""
            from whenever import TimeDelta

            from utilities.pytest import throttle

            @throttle(root={str(tmp_path)!r}, delta=TimeDelta(seconds={self.delta}))
            def test_main() -> None:
                assert True
            """
        )
        testdir.runpytest().assert_outcomes(passed=1)
        path = one(tmp_path.iterdir())
        _ = path.write_text("invalid")
        testdir.runpytest().assert_outcomes(passed=1)
