from __future__ import annotations

from inspect import signature
from time import sleep
from typing import TYPE_CHECKING

import pytest

from tests.conftest import FLAKY
from utilities.pytest import throttle

if TYPE_CHECKING:
    from pathlib import Path

    from _pytest.legacypath import Testdir

    from utilities.types import IterableStrs


class TestPytestOptions:
    def test_unknown_mark(self, *, testdir: Testdir) -> None:
        _ = testdir.makepyfile(
            """
            import pytest

            @pytest.mark.unknown
            def test_main():
                assert True
            """
        )
        result = testdir.runpytest()
        result.assert_outcomes(errors=1)
        result.stdout.re_match_lines([r".*Unknown pytest\.mark\.unknown"])

    @pytest.mark.parametrize("configure", [pytest.param(True), pytest.param(False)])
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
            from pytest import mark

            def test_main():
                assert True
            """
        )
        result = testdir.runpytest("--unknown")
        result.stderr.re_match_lines([r".*unrecognized arguments.*"])

    @pytest.mark.parametrize(
        ("case", "passed", "skipped", "matches"),
        [
            pytest.param([], 0, 1, [".*3: pass --slow"]),
            pytest.param(["--slow"], 1, 0, []),
        ],
    )
    def test_one_mark_and_option(
        self,
        *,
        testdir: Testdir,
        case: IterableStrs,
        passed: int,
        skipped: int,
        matches: IterableStrs,
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
            import pytest

            @pytest.mark.slow
            def test_main():
                assert True
            """
        )
        result = testdir.runpytest("-rs", *case)
        result.assert_outcomes(passed=passed, skipped=skipped)
        result.stdout.re_match_lines(list(matches))

    @pytest.mark.parametrize(
        ("case", "passed", "skipped", "matches"),
        [
            pytest.param(
                [],
                1,
                3,
                [
                    "SKIPPED.*: pass --slow",
                    "SKIPPED.*: pass --fast",
                    "SKIPPED.*: pass --slow --fast",
                ],
            ),
            pytest.param(
                ["--slow"],
                2,
                2,
                ["SKIPPED.*: pass --fast", "SKIPPED.*: pass --slow --fast"],
            ),
            pytest.param(
                ["--fast"],
                2,
                2,
                ["SKIPPED.*: pass --slow", "SKIPPED.*: pass --slow --fast"],
            ),
            pytest.param(["--slow", "--fast"], 4, 0, []),
        ],
    )
    def test_two_marks_and_options(
        self,
        *,
        testdir: Testdir,
        case: IterableStrs,
        passed: int,
        skipped: int,
        matches: IterableStrs,
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
            import pytest

            def test_none():
                assert True

            @pytest.mark.slow
            def test_slow():
                assert True

            @pytest.mark.fast
            def test_fast():
                assert True

            @pytest.mark.slow
            @pytest.mark.fast
            def test_both():
                assert True
            """
        )
        result = testdir.runpytest("-rs", *case, "--randomly-dont-reorganize")
        result.assert_outcomes(passed=passed, skipped=skipped)
        result.stdout.re_match_lines(list(matches))


class TestThrottle:
    @FLAKY
    @pytest.mark.parametrize("as_float", [pytest.param(True), pytest.param(False)])
    @pytest.mark.parametrize("on_try", [pytest.param(True), pytest.param(False)])
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

    @FLAKY
    def test_on_pass(self, *, testdir: Testdir, tmp_path: Path) -> None:
        _ = testdir.makeconftest(
            """
            import pytest

            def pytest_addoption(parser):
                parser.addoption("--pass", action="store_true")

            @pytest.fixture
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

    @FLAKY
    def test_on_try(self, *, testdir: Testdir, tmp_path: Path) -> None:
        _ = testdir.makeconftest(
            """
            import pytest

            def pytest_addoption(parser):
                parser.addoption("--pass", action="store_true")

            @pytest.fixture
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

    @FLAKY
    def test_long_name(self, *, testdir: Testdir, tmp_path: Path) -> None:
        root_str = str(tmp_path)
        contents = f"""
            import pytest

            from string import printable
            from utilities.pytest import throttle

            @pytest.mark.parametrize('arg', [10 * printable])
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
