from __future__ import annotations

import re
from os import environ
from re import MULTILINE, Pattern

from pytest import fixture, mark

from utilities.platform import IS_NOT_LINUX, IS_WINDOWS
from utilities.text import strip_and_dedent

FLAKY = mark.flaky(reruns=5, reruns_delay=1)
IS_CI = "CI" in environ
SKIPIF_CI = mark.skipif(IS_CI, reason="Skipped for CI")
IS_CI_AND_WINDOWS = IS_CI and IS_WINDOWS
SKIPIF_CI_AND_WINDOWS = mark.skipif(IS_CI_AND_WINDOWS, reason="Skipped for CI/Windows")
SKIPIF_CI_AND_NOT_LINUX = mark.skipif(
    IS_CI and IS_NOT_LINUX, reason="Skipped for CI/non-Linux"
)


# hypothesis


try:
    from utilities.hypothesis import setup_hypothesis_profiles
except ModuleNotFoundError:
    pass
else:
    setup_hypothesis_profiles()


# fixtures


@fixture
def traceback_func_chain() -> Pattern[str]:
    return re.compile(
        strip_and_dedent(
            r"""
            Date/time \| .+
            User      \| .+
            Host      \| .+
            Version   \|\s

            Exception chain 1/2:
                Frame 1/1: func_chain_first \(tests\.test_traceback_funcs\.chain\)
                    Inputs:
                        args\[0\] = 1
                        args\[1\] = 2
                        args\[2\] = 3
                        args\[3\] = 4
                        kwargs\[c\] = 5
                        kwargs\[d\] = 6
                        kwargs\[e\] = 7
                    Locals:
                        a = 2
                        b = 4
                        c = 10
                        args = \(6, 8\)
                        kwargs = {'d': 12, 'e': 14}
                        msg = 'Assertion failed: Result \(112\) must be divisible by 10'
                    Line 19:
                        raise ValueError\(msg\) from error
                    Raised:
                        builtins\.ValueError\(Assertion failed: Result \(112\) must be divisible by 10\)

            Exception chain 2/2:
                Frame 1/1: func_chain_second \(tests\.test_traceback_funcs\.chain\)
                    Inputs:
                        args\[0\] = 2
                        args\[1\] = 4
                        args\[2\] = 6
                        args\[3\] = 8
                        kwargs\[c\] = 10
                        kwargs\[d\] = 12
                        kwargs\[e\] = 14
                    Locals:
                        a = 4
                        b = 8
                        c = 20
                        args = \(12, 16\)
                        kwargs = {'d': 24, 'e': 28}
                        result = 112
                    Line 30:
                        assert result % 10 == 0, f"Result \({result}\) must be divisible by 10"
                    Raised:
                        builtins\.AssertionError\(Result \(112\) must be divisible by 10\)
            """
        ),
        flags=MULTILINE,
    )


@fixture
def traceback_func_one() -> Pattern[str]:
    return re.compile(
        strip_and_dedent(
            r"""
            Date/time \| .+
            User      \| .+
            Host      \| .+
            Version   \|\s

            Frame 1/1: func_one \(tests\.test_traceback_funcs\.one\)
                Inputs:
                    args\[0\] = 1
                    args\[1\] = 2
                    args\[2\] = 3
                    args\[3\] = 4
                    kwargs\[c\] = 5
                    kwargs\[d\] = 6
                    kwargs\[e\] = 7
                Locals:
                    a = 2
                    b = 4
                    c = 10
                    args = \(6, 8\)
                    kwargs = {'d': 12, 'e': 14}
                    result = 56
                Line 16:
                    assert result % 10 == 0, f"Result \({result}\) must be divisible by 10"
                Raised:
                    builtins\.AssertionError\(Result \(56\) must be divisible by 10\)
            """
        ),
        flags=MULTILINE,
    )


@fixture
def traceback_func_many_long() -> Pattern[str]:
    return re.compile(
        strip_and_dedent(
            r"""
            Date/time \| .+
            User      \| .+
            Host      \| .+
            Version   \|\s

            Frame 1/1: func_many \(tests.test_traceback_funcs.many\)
                Inputs:
                    args\[0\] = 1
                    args\[1\] = 2
                    args\[2\] = 3
                    args\[3\] = 4
                    kwargs\[c\] = 5
                    kwargs\[d\] = 6
                    kwargs\[e\] = 7
                Locals:
                    a = 2
                    b = 4
                    c = 10
                    args = \(
                        6,
                        8,
                        0,
                        2,
                        4,
                        6,
                        8,
                        10,
                        12,
                        14,
                        16,
                        18,
                        20,
                        22,
                        24,
                        26,
                        28,
                        30,
                        32,
                        34,
                        ... \+82
                    \)
                    kwargs = {'d': 12, 'e': 14}
                    result = 9956
                Line 16:
                    assert result % 10 == 0, f"Result \({result}\) must be divisible by 10"
                Raised:
                    builtins\.AssertionError\(Result \(9956\) must be divisible by 10\)
            """
        ),
        flags=MULTILINE,
    )


@fixture
def traceback_func_many_short() -> Pattern[str]:
    return re.compile(
        strip_and_dedent(
            r"""
            Date/time \| .+
            User      \| .+
            Host      \| .+
            Version   \|\s

            Frame 1/1: func_many \(tests.test_traceback_funcs.many\)
                Inputs:
                    args\[0\] = 1
                    args\[1\] = 2
                    args\[2\] = 3
                    args\[3\] = 4
                    kwargs\[c\] = 5
                    kwargs\[d\] = 6
                    kwargs\[e\] = 7
                Locals:
                    a = 2
                    b = 4
                    c = 10
                    args = \(6, 8, 0, 2, 4, ... \+97\)
                    kwargs = {'d': 12, 'e': 14}
                    result = 9956
                Line 16:
                    assert result % 10 == 0, f"Result \({result}\) must be divisible by 10"
                Raised:
                    builtins\.AssertionError\(Result \(9956\) must be divisible by 10\)
            """
        ),
        flags=MULTILINE,
    )


@fixture
def traceback_func_task_group_one() -> Pattern[str]:
    return re.compile(
        strip_and_dedent(
            r"""
            Date/time \| .+
            User      \| .+
            Host      \| .+
            Version   \|\s

            Exception group:
                Frame 1/1: func_task_group_one_first \(tests\.test_traceback_funcs\.task_group_one\)
                    Inputs:
                        args\[0\] = 1
                        args\[1\] = 2
                        args\[2\] = 3
                        args\[3\] = 4
                        kwargs\[c\] = 5
                        kwargs\[d\] = 6
                        kwargs\[e\] = 7
                    Locals:
                        a = 2
                        b = 4
                        c = 10
                        args = \(6, 8\)
                        kwargs = {'d': 12, 'e': 14}
                        tg = <TaskGroup cancelling>
                        _ = <Task finished name='Task-\d+' coro=<func_task_group_one_second\(\) done, defined at .+src.+utilities.+traceback\.py:\d+> exception=AssertionError\('Result \(112\) must be divisible by 10'\)>
                    Line 18:
                        async with TaskGroup\(\) as tg:
                    Raised:
                        builtins\.ExceptionGroup\(unhandled errors in a TaskGroup \(1 sub-exception\)\)

                Exception group error 1/1:
                    Frame 1/1: func_task_group_one_second \(tests\.test_traceback_funcs\.task_group_one\)
                        Inputs:
                            args\[0\] = 2
                            args\[1\] = 4
                            args\[2\] = 6
                            args\[3\] = 8
                            kwargs\[c\] = 10
                            kwargs\[d\] = 12
                            kwargs\[e\] = 14
                        Locals:
                            a = 4
                            b = 8
                            c = 20
                            args = \(12, 16\)
                            kwargs = {'d': 24, 'e': 28}
                            result = 112
                        Line 33:
                            assert result % 10 == 0, f"Result \({result}\) must be divisible by 10"
                        Raised:
                            builtins\.AssertionError\(Result \(112\) must be divisible by 10\)
            """
        )
    )


@fixture
def traceback_func_two() -> Pattern[str]:
    return re.compile(
        strip_and_dedent(
            r"""
            Date/time \| .+
            User      \| .+
            Host      \| .+
            Version   \|\s

            Frame 1/2: func_two_first \(tests\.test_traceback_funcs\.two\)
                Inputs:
                    args\[0\] = 1
                    args\[1\] = 2
                    args\[2\] = 3
                    args\[3\] = 4
                    kwargs\[c\] = 5
                    kwargs\[d\] = 6
                    kwargs\[e\] = 7
                Locals:
                    a = 2
                    b = 4
                    c = 10
                    args = \(6, 8\)
                    kwargs = {'d': 12, 'e': 14}
                Line 15:
                    return func_two_second\(a, b, \*args, c=c, \*\*kwargs\)

            Frame 2/2: func_two_second \(tests\.test_traceback_funcs\.two\)
                Inputs:
                    args\[0\] = 2
                    args\[1\] = 4
                    args\[2\] = 6
                    args\[3\] = 8
                    kwargs\[c\] = 10
                    kwargs\[d\] = 12
                    kwargs\[e\] = 14
                Locals:
                    a = 4
                    b = 8
                    c = 20
                    args = \(12, 16\)
                    kwargs = {'d': 24, 'e': 28}
                    result = 112
                Line 26:
                    assert result % 10 == 0, f"Result \({result}\) must be divisible by 10"
                Raised:
                    builtins\.AssertionError\(Result \(112\) must be divisible by 10\)
            """
        ),
        flags=MULTILINE,
    )


@fixture
def traceback_func_untraced() -> Pattern[str]:
    return re.compile(
        strip_and_dedent(
            r"""
            Traceback \(most recent call last\):

              File ".+src.+tests.+test_(logging|sys|traceback)\.py", line \d+, in test_.+
                _ = func_untraced\(1, 2, 3, 4, c=5, d=6, e=7\)
                    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

              File ".+src.+tests.+test_traceback_funcs.+untraced\.py", line 13, in func_untraced
                assert result % 10 == 0, f"Result \({result}\) must be divisible by 10"
                       ^^^^^^^^^^^^^^^^

            AssertionError: Result \(56\) must be divisible by 10
            """
        ).replace("^", r"\^"),
        flags=MULTILINE,
    )
