from os import environ
from typing import Any

from pytest import mark, param

from utilities.pytest import is_pytest


class TestPytestOptions:
    def test_unknown_mark(self, testdir: Any) -> None:
        testdir.makepyfile(
            """
            from pytest import mark

            @mark.slow
            def test_main():
                assert True
            """,
        )
        result = testdir.runpytest()
        result.assert_outcomes(errors=1)
        result.stdout.re_match_lines([r".*Unknown pytest\.mark\.slow"])

    def test_configured_mark_unknown_option(self, testdir: Any) -> None:
        testdir.makeconftest(
            """
            from utilities.pytest import add_pytest_configure

            def pytest_configure(config):
                add_pytest_configure(config, [("slow", "slow to run")])
            """,
        )
        testdir.makepyfile(
            """
            from pytest import mark

            @mark.slow
            def test_main():
                assert True
            """,
        )
        result = testdir.runpytest("--slow")
        result.stderr.re_match_lines(
            ["-c: error: unrecognized arguments: --slow"],
        )

    @mark.parametrize(
        ("case", "passed", "skipped", "matches"),
        [
            param([], 0, 1, [".*3: pass --slow"]),
            param(["--slow"], 1, 0, []),
        ],
    )
    def test_configured_one_mark_and_option(
        self,
        testdir: Any,
        case: list[str],
        passed: int,
        skipped: int,
        matches: list[str],
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
            """,
        )
        testdir.makepyfile(
            """
            from pytest import mark

            @mark.slow
            def test_main():
                assert True
            """,
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
                    ".*6: pass --slow",
                    ".*10: pass --fast",
                    ".*14: pass --slow --fast",
                ],
            ),
            param(
                ["--slow"],
                2,
                2,
                [".*10: pass --fast", ".*14: pass --slow --fast"],
            ),
            param(
                ["--fast"],
                2,
                2,
                [".*6: pass --slow", ".*14: pass --slow --fast"],
            ),
            param(["--slow", "--fast"], 4, 0, []),
        ],
    )
    def test_configured_two_marks_and_options(
        self,
        testdir: Any,
        case: list[str],
        passed: int,
        skipped: int,
        matches: list[str],
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
            """,
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
            """,
        )
        result = testdir.runpytest("-rs", *case)
        result.assert_outcomes(passed=passed, skipped=skipped)
        result.stdout.re_match_lines(matches)


class TestIsPytest:
    def test_main(self) -> None:
        assert is_pytest()

    def test_disable(self) -> None:
        del environ["PYTEST_CURRENT_TEST"]
        assert not is_pytest()
