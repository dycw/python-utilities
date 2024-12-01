from __future__ import annotations

import sys
from subprocess import PIPE, CalledProcessError, check_output

from pytest import raises

from utilities.sys import VERSION_MAJOR_MINOR
from utilities.text import strip_and_dedent


class TestExceptHook:
    def test_custom_excepthook(self) -> None:
        code = strip_and_dedent("""
            from __future__ import annotations

            import sys
            from itertools import chain
            from logging import getLogger

            from utilities.sys import log_exception_paths
            from utilities.traceback import trace

            sys.excepthook = log_exception_paths


            @trace
            def first(a: int, b: int, /, *args: int, c: int = 0, **kwargs: int) -> int:
                a *= 2
                b *= 2
                args = tuple(2 * arg for arg in args)
                c *= 2
                kwargs = {k: 2 * v for k, v in kwargs.items()}
                return second(a, b, *args, c, **kwargs)


            @trace
            def second(a: int, b: int, /, *args: int, c: int = 0, **kwargs: int) -> int:
                a *= 2
                b *= 2
                args = tuple(2 * arg for arg in args)
                c *= 2
                kwargs = {k: 2 * v for k, v in kwargs.items()}
                result = sum(chain([a, b], args, [c], kwargs.values()))
                assert result % 10 == 0, f"Result ({result}) must be divisible by 10"
                return result


            _ = first(1, 2, 3, 4, c=5, d=6, e=7)
            """)
        with raises(CalledProcessError) as exc_info:
            _ = check_output([sys.executable, "-c", code], stderr=PIPE, text=True)
        stderr = exc_info.value.stderr.strip("\n")
        expected = strip_and_dedent("""
            ExcPath(
                frames=[
                    _Frame(
                        module='__main__',
                        name='first',
                        code_line='',
                        line_num=20,
                        args=(1, 2, 3, 4),
                        kwargs={'c': 5, 'd': 6, 'e': 7},
                        locals={
                            'a': 2,
                            'b': 4,
                            'c': 10,
                            'args': (6, 8),
                            'kwargs': {'d': 12, 'e': 14}
                        }
                    ),
                    _Frame(
                        module='__main__',
                        name='second',
                        code_line='',
                        line_num=31,
                        args=(2, 4, 6, 8, 10),
                        kwargs={'d': 12, 'e': 14},
                        locals={
                            'a': 4,
                            'b': 8,
                            'c': 0,
                            'args': (12, 16, 20),
                            'kwargs': {'d': 24, 'e': 28},
                            'result': 112
                        }
                    )
                ],
                error=AssertionError('Result (112) must be divisible by 10')
            )
            """)
        assert stderr == expected


class TestVersionMajorMinor:
    def test_main(self) -> None:
        assert isinstance(VERSION_MAJOR_MINOR, tuple)
        expected = 2
        assert len(VERSION_MAJOR_MINOR) == expected
