from __future__ import annotations

from inspect import signature
from pathlib import Path
from time import sleep
from typing import Any

from pytest import MonkeyPatch, mark, param

from utilities.pytest import is_pytest, throttle
from utilities.text import strip_and_dedent
from utilities.typing import IterableStrs


class TestPytestOptions:
    def test_unknown_mark(self, *, testdir: Any) -> None:
        testdir.makepyfile(
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
    def test_unknown_option(self, *, configure: bool, testdir: Any) -> None:
        if configure:
            testdir.makeconftest(
                """
                from utilities.pytest import add_pytest_configure

                def pytest_configure(config):
                    add_pytest_configure(config, [("slow", "slow to run")])
                """
            )
        testdir.makepyfile(
            """
            from pytest import mark

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
        testdir: Any,
        case: IterableStrs,
        passed: int,
        skipped: int,
        matches: IterableStrs,
    ) -> None:
        testdir.makeconftest(
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
        testdir.makepyfile(
            """
            from pytest import mark

            @mark.slow
            def test_main():
                assert True
            """
        )
        result = testdir.runpytest("-rs", *case)
        result.assert_outcomes(passed=passed, skipped=skipped)
        result.stdout.re_match_lines(matches)

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
        testdir: Any,
        case: IterableStrs,
        passed: int,
        skipped: int,
        matches: IterableStrs,
    ) -> None:
        testdir.makeconftest(
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
        testdir.makepyfile(
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
        result.stdout.re_match_lines(matches)


class TestIsPytest:
    def test_function(self) -> None:
        assert is_pytest()

    def test_disable(self, *, monkeypatch: MonkeyPatch) -> None:
        monkeypatch.delenv("PYTEST_CURRENT_TEST")
        assert not is_pytest()


class TestThrottle:
    def test_duration_as_float(self, *, testdir: Any, tmp_path: Path) -> None:
        root_str = str(tmp_path)
        contents = f"""
            from utilities.pytest import throttle

            @throttle(root={root_str!r}, duration=1.0)
            def test_main():
                assert True
            """
        self._test_throttle(testdir, contents)

    def test_duration_as_timedelta(self, *, testdir: Any, tmp_path: Path) -> None:
        root_str = str(tmp_path)
        contents = f"""
            import datetime as dt
            from utilities.pytest import throttle

            @throttle(root={root_str!r}, duration=dt.timedelta(seconds=1.0))
            def test_main():
                assert True
            """
        self._test_throttle(testdir, contents)

    def _test_throttle(self, testdir: Any, contents: str, /) -> None:
        testdir.makepyfile(strip_and_dedent(contents))

        result = testdir.runpytest()
        result.assert_outcomes(passed=1)

        result = testdir.runpytest()
        result.assert_outcomes(skipped=1)

        sleep(1.0)
        result = testdir.runpytest()
        result.assert_outcomes(passed=1)

    def test_signature(self) -> None:
        @throttle()
        def func(*, fix: bool) -> None:
            assert fix

        def other(*, fix: bool) -> None:
            assert fix

        assert signature(func) == signature(other)
