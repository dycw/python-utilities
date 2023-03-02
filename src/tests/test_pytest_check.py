from typing import Any


class TestCheck:
    def test_regular(self, testdir: Any) -> None:
        testdir.makepyfile(
            """
            from utilities.pytest_check import check

            def test_main():
                with check():
                    assert False, "first"
                with check():
                    assert False, "second"
            """
        )
        result = testdir.runpytest()
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines(
            ["FAILURE: first", "FAILURE: second", "Failed Checks: 2"]
        )

    def test_fail_on_first(self, testdir: Any) -> None:
        testdir.makepyfile(
            """
            from utilities.os import temp_environ
            from utilities.pytest_check import check

            def test_main():
                with temp_environ({"PYTEST_CHECK": "0"}):
                    with check():
                        assert False, "first"
                    with check():
                        assert False, "second"
            """
        )
        result = testdir.runpytest()
        result.assert_outcomes(failed=1)
        result.stdout.re_match_lines([".*AssertionError: first"])
