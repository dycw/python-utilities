from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from pytest import raises

from tests.test_traceback_funcs.chain import func_chain_first
from tests.test_traceback_funcs.decorated_async import func_decorated_async_first
from tests.test_traceback_funcs.decorated_sync import func_decorated_sync_first
from tests.test_traceback_funcs.error_bind import (
    func_error_bind_async,
    func_error_bind_sync,
)
from tests.test_traceback_funcs.one import func_one
from tests.test_traceback_funcs.recursive import func_recursive
from tests.test_traceback_funcs.task_group import func_task_group_first
from tests.test_traceback_funcs.two import func_two_first
from tests.test_traceback_funcs.untraced import func_untraced
from utilities.iterables import OneNonUniqueError, one
from utilities.text import ensure_str, strip_and_dedent
from utilities.traceback import (
    ExcChain,
    ExcGroup,
    ExcPath,
    _CallArgsError,
    assemble_exception_paths,
    trace,
    yield_exceptions,
    yield_extended_frame_summaries,
    yield_frames,
)

if TYPE_CHECKING:
    from traceback import FrameSummary
    from types import FrameType


class TestAssembleExceptionsPaths:
    def test_func_one(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_one(1, 2, 3, 4, c=5, d=6, e=7)
        exc_path = assemble_exception_paths(exc_info.value)
        assert isinstance(exc_path, ExcPath)
        assert len(exc_path) == 1
        frame = one(exc_path)
        assert frame.module == "tests.test_traceback_funcs.one"
        assert frame.name == "func_one"
        assert (
            frame.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert frame.line_num == 16
        assert frame.args == (1, 2, 3, 4)
        assert frame.kwargs == {"c": 5, "d": 6, "e": 7}
        assert set(frame.locals) == {"a", "b", "c", "args", "kwargs", "result"}
        assert frame.locals["a"] == 2
        assert frame.locals["b"] == 4
        assert frame.locals["args"] == (6, 8)
        assert frame.locals["kwargs"] == {"d": 12, "e": 14}
        assert isinstance(exc_path.error, AssertionError)

    def test_func_two(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_two_first(1, 2, 3, 4, c=5, d=6, e=7)
        exc_path = assemble_exception_paths(exc_info.value)
        assert isinstance(exc_path, ExcPath)
        assert len(exc_path) == 2
        frame1, frame2 = exc_path
        assert frame1.module == "tests.test_traceback_funcs.two"
        assert frame1.name == "func_two_first"
        assert frame1.code_line == "return func_two_second(a, b, *args, c=c, **kwargs)"
        assert frame1.args == (1, 2, 3, 4)
        assert frame1.kwargs == {"c": 5, "d": 6, "e": 7}
        assert frame1.locals["a"] == 2
        assert frame1.locals["b"] == 4
        assert frame1.locals["args"] == (6, 8)
        assert frame1.locals["kwargs"] == {"d": 12, "e": 14}
        assert frame2.module == "tests.test_traceback_funcs.two"
        assert frame2.name == "func_two_second"
        assert (
            frame2.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert frame2.args == (2, 4, 6, 8)
        assert frame2.kwargs == {"c": 10, "d": 12, "e": 14}
        assert frame2.locals["a"] == 4
        assert frame2.locals["b"] == 8
        assert frame2.locals["args"] == (12, 16)
        assert frame2.locals["kwargs"] == {"d": 24, "e": 28}
        assert isinstance(exc_path.error, AssertionError)

    def test_func_chain(self) -> None:
        with raises(ValueError, match=".*") as exc_info:
            _ = func_chain_first(1, 2, 3, 4, c=5, d=6, e=7)
        exc_chain = assemble_exception_paths(exc_info.value)
        assert isinstance(exc_chain, ExcChain)
        assert len(exc_chain) == 2
        path1, path2 = exc_chain
        assert isinstance(path1, ExcPath)
        assert len(path1) == 1
        frame1 = one(path1)
        assert frame1.module == "tests.test_traceback_funcs.chain"
        assert frame1.name == "func_chain_second"
        assert (
            frame1.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert frame1.args == (2, 4, 6, 8)
        assert frame1.kwargs == {"c": 10, "d": 12, "e": 14}
        assert frame1.locals["a"] == 4
        assert frame1.locals["b"] == 8
        assert frame1.locals["args"] == (12, 16)
        assert frame1.locals["kwargs"] == {"d": 24, "e": 28}
        assert isinstance(path2, ExcPath)
        frame2 = one(path2)
        assert frame2.module == "tests.test_traceback_funcs.chain"
        assert frame2.name == "func_chain_first"
        assert frame2.code_line == "raise ValueError(msg) from error"
        assert frame2.args == (1, 2, 3, 4)
        assert frame2.kwargs == {"c": 5, "d": 6, "e": 7}
        assert frame2.locals["a"] == 2
        assert frame2.locals["b"] == 4
        assert frame2.locals["args"] == (6, 8)
        assert frame2.locals["kwargs"] == {"d": 12, "e": 14}

    def test_func_decorated_sync(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_decorated_sync_first(1, 2, 3, 4, c=5, d=6, e=7)
        exc_path = assemble_exception_paths(exc_info.value)
        assert isinstance(exc_path, ExcPath)
        self._assert_decorated(exc_path, "sync")
        assert len(exc_path) == 5

    async def test_func_decorated_async(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = await func_decorated_async_first(1, 2, 3, 4, c=5, d=6, e=7)
        error = assemble_exception_paths(exc_info.value)
        assert isinstance(error, ExcPath)
        self._assert_decorated(error, "async")

    def test_func_recursive(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_recursive(1, 2, 3, 4, c=5, d=6, e=7)
        exc_path = assemble_exception_paths(exc_info.value)
        assert isinstance(exc_path, ExcPath)
        assert len(exc_path) == 2
        first, second = exc_path
        assert first.module == "tests.test_traceback_funcs.recursive"
        assert first.name == "func_recursive"
        assert first.code_line == "return func_recursive(a, b, *args, c=c, **kwargs)"
        assert first.args == (1, 2, 3, 4)
        assert first.kwargs == {"c": 5, "d": 6, "e": 7}
        assert first.locals["a"] == 2
        assert first.locals["b"] == 4
        assert first.locals["args"] == (6, 8)
        assert first.locals["kwargs"] == {"d": 12, "e": 14}
        assert second.module == "tests.test_traceback_funcs.recursive"
        assert second.name == "func_recursive"
        assert (
            second.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert second.args == (2, 4, 6, 8)
        assert second.kwargs == {"c": 10, "d": 12, "e": 14}
        assert second.locals["a"] == 4
        assert second.locals["b"] == 8
        assert second.locals["args"] == (12, 16)
        assert second.locals["kwargs"] == {"d": 24, "e": 28}
        assert isinstance(exc_path.error, AssertionError)

    async def test_func_task_group(self) -> None:
        with raises(ExceptionGroup) as exc_info:
            await func_task_group_first(1, 2, 3, 4, c=5, d=6, e=7)
        exc_group = assemble_exception_paths(exc_info.value)
        assert isinstance(exc_group, ExcGroup)
        assert exc_group.path is not None
        assert len(exc_group.path) == 1
        path_frame = one(exc_group.path)
        assert path_frame.module == "tests.test_traceback_funcs.task_group"
        assert path_frame.name == "func_task_group_first"
        assert path_frame.code_line == "async with TaskGroup() as tg:"
        assert path_frame.args == (1, 2, 3, 4)
        assert path_frame.kwargs == {"c": 5, "d": 6, "e": 7}
        assert path_frame.locals["a"] == 2
        assert path_frame.locals["b"] == 4
        assert path_frame.locals["args"] == (6, 8)
        assert path_frame.locals["kwargs"] == {"d": 12, "e": 14}
        assert isinstance(exc_group.path.error, ExceptionGroup)
        assert len(exc_group.errors) == 2
        first, second = exc_group.errors
        assert isinstance(first, ExcPath)
        assert len(first) == 1
        first_frame = one(first)
        assert first_frame.module == "tests.test_traceback_funcs.task_group"
        assert first_frame.name == "func_task_group_second"
        assert (
            first_frame.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert first_frame.args == (2, 4, 6, 8)
        assert first_frame.kwargs == {"c": 10, "d": 12, "e": 14}
        assert first_frame.locals["a"] == 4
        assert first_frame.locals["b"] == 8
        assert first_frame.locals["args"] == (12, 16)
        assert first_frame.locals["kwargs"] == {"d": 24, "e": 28}
        assert isinstance(first.error, AssertionError)
        assert isinstance(second, ExcPath)
        assert len(second) == 1
        second_frame = one(second)
        assert second_frame.module == "tests.test_traceback_funcs.task_group"
        assert second_frame.name == "func_task_group_second"
        assert (
            second_frame.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert second_frame.args == (3, 5, 7, 9)
        assert second_frame.kwargs == {"c": 11, "d": 13, "e": 15}
        assert second_frame.locals["a"] == 6
        assert second_frame.locals["b"] == 10
        assert second_frame.locals["args"] == (14, 18)
        assert second_frame.locals["kwargs"] == {"d": 26, "e": 30}
        assert isinstance(second.error, AssertionError)

    def test_func_untraced(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_untraced(1, 2, 3, 4, c=5, d=6, e=7)
        error = assemble_exception_paths(exc_info.value)
        assert isinstance(error, AssertionError)

    def test_custom_error(self) -> None:
        @trace
        def raises_custom_error() -> bool:
            return one([True, False])

        with raises(OneNonUniqueError) as exc_info:
            _ = raises_custom_error()
        exc_path = assemble_exception_paths(exc_info.value)
        assert isinstance(exc_path, ExcPath)
        assert exc_path.error.first is True
        assert exc_path.error.second is False

    def test_error_bind_sync(self) -> None:
        with raises(_CallArgsError) as exc_info:
            _ = func_error_bind_sync(1)  # pyright: ignore[reportCallIssue]
        msg = ensure_str(one(exc_info.value.args))
        expected = strip_and_dedent(
            """
            Unable to bind arguments for 'func_error_bind_sync'; missing a required argument: 'b'
            args[0] = 1
            """
        )
        assert msg == expected

    async def test_error_bind_async(self) -> None:
        with raises(_CallArgsError) as exc_info:
            _ = await func_error_bind_async(1, 2, 3)  # pyright: ignore[reportCallIssue]
        msg = ensure_str(one(exc_info.value.args))
        expected = strip_and_dedent(
            """
            Unable to bind arguments for 'func_error_bind_async'; too many positional arguments
            args[0] = 1
            args[1] = 2
            args[2] = 3
            """
        )
        assert msg == expected

    def _assert_decorated(
        self, exc_path: ExcPath, sync_or_async: Literal["sync", "async"], /
    ) -> None:
        assert len(exc_path) == 5
        first, second, _, fourth, fifth = exc_path
        match sync_or_async:
            case "sync":
                maybe_await = ""
            case "async":
                maybe_await = "await "
        assert first.module == f"tests.test_traceback_funcs.decorated_{sync_or_async}"
        assert first.name == f"func_decorated_{sync_or_async}_first"
        assert (
            first.code_line
            == f"return {maybe_await}func_decorated_{sync_or_async}_second(a, b, *args, c=c, **kwargs)"
        )
        assert first.args == (1, 2, 3, 4)
        assert first.kwargs == {"c": 5, "d": 6, "e": 7}
        assert first.locals["a"] == 2
        assert first.locals["b"] == 4
        assert first.locals["args"] == (6, 8)
        assert first.locals["kwargs"] == {"d": 12, "e": 14}
        assert second.module == f"tests.test_traceback_funcs.decorated_{sync_or_async}"
        assert second.name == f"func_decorated_{sync_or_async}_second"
        assert (
            second.code_line
            == f"return {maybe_await}func_decorated_{sync_or_async}_third(a, b, *args, c=c, **kwargs)"
        )
        assert second.args == (2, 4, 6, 8)
        assert second.kwargs == {"c": 10, "d": 12, "e": 14}
        assert second.locals["a"] == 4
        assert second.locals["b"] == 8
        assert second.locals["args"] == (12, 16)
        assert second.locals["kwargs"] == {"d": 24, "e": 28}
        assert fourth.module == f"tests.test_traceback_funcs.decorated_{sync_or_async}"
        assert fourth.name == f"func_decorated_{sync_or_async}_fourth"
        assert (
            fourth.code_line
            == f"return {maybe_await}func_decorated_{sync_or_async}_fifth(a, b, *args, c=c, **kwargs)"
        )
        assert fourth.args == (8, 16, 24, 32)
        assert fourth.kwargs == {"c": 40, "d": 48, "e": 56}
        assert fourth.locals["a"] == 16
        assert fourth.locals["b"] == 32
        assert fourth.locals["args"] == (48, 64)
        assert fourth.locals["kwargs"] == {"d": 96, "e": 112}
        assert fifth.module == f"tests.test_traceback_funcs.decorated_{sync_or_async}"
        assert fifth.name == f"func_decorated_{sync_or_async}_fifth"
        assert (
            fifth.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert fifth.args == (16, 32, 48, 64)
        assert fifth.kwargs == {"c": 80, "d": 96, "e": 112}
        assert fifth.locals["a"] == 32
        assert fifth.locals["b"] == 64
        assert fifth.locals["args"] == (96, 128)
        assert fifth.locals["kwargs"] == {"d": 192, "e": 224}
        assert isinstance(exc_path.error, AssertionError)


from pytest import mark


class TestTrace:
    @mark.xfail
    def test_pretty(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_two_first(1, 2, 3, 4, c=5, d=6, e=7)
        error = exc_info.value
        result = error.pretty(location=False)
        expected = strip_and_dedent("""
            Error running:

              1. func_two_first
              2. func_two_second
              >> AssertionError: Result (112) must be divisible by 10

            Frames:

              1/2. func_two_first

                Inputs:

                  args[0] = 1
                  args[1] = 2
                  args[2] = 3
                  args[3] = 4
                  kwargs[c] = 5
                  kwargs[d] = 6
                  kwargs[e] = 7

                Locals:

                  a = 2
                  b = 4
                  c = 10
                  args = (6, 8)
                  kwargs = {'d': 12, 'e': 14}

                >> return func_two_second(a, b, *args, c=c, **kwargs)

              2/2. func_two_second

                Inputs:

                  args[0] = 2
                  args[1] = 4
                  args[2] = 6
                  args[3] = 8
                  kwargs[c] = 10
                  kwargs[d] = 12
                  kwargs[e] = 14

                Locals:

                  a = 4
                  b = 8
                  c = 20
                  args = (12, 16)
                  kwargs = {'d': 24, 'e': 28}
                  result = 112

                >> assert result % 10 == 0, f"Result ({result}) must be divisible by 10"
                >> AssertionError: Result (112) must be divisible by 10
        """)
        assert result == expected


class TestYieldExceptions:
    def test_main(self) -> None:
        class FirstError(Exception): ...

        class SecondError(Exception): ...

        def f() -> None:
            try:
                return g()
            except FirstError:
                raise SecondError from FirstError

        def g() -> None:
            raise FirstError

        with raises(SecondError) as exc_info:
            f()
        errors = list(yield_exceptions(exc_info.value))
        assert len(errors) == 2
        first, second = errors
        assert isinstance(first, SecondError)
        assert isinstance(second, FirstError)


class TestYieldExtendedFrameSummaries:
    def test_main(self) -> None:
        def f() -> None:
            return g()

        def g() -> None:
            raise NotImplementedError

        try:
            f()
        except NotImplementedError as error:
            frames = list(yield_extended_frame_summaries(error))
            assert len(frames) == 3
            expected = [
                TestYieldExtendedFrameSummaries.test_main.__qualname__,
                f.__qualname__,
                g.__qualname__,
            ]
            for frame, exp in zip(frames, expected, strict=True):
                assert frame.qualname == exp
        else:
            msg = "Expected an error"
            raise RuntimeError(msg)

    def test_extra(self) -> None:
        def f() -> None:
            return g()

        def g() -> None:
            raise NotImplementedError

        def extra(summary: FrameSummary, frame: FrameType, /) -> tuple[int | None, int]:
            left = None if summary.locals is None else len(summary.locals)
            return left, len(frame.f_locals)

        try:
            f()
        except NotImplementedError as error:
            frames = list(yield_extended_frame_summaries(error, extra=extra))
            assert len(frames) == 3
            expected = [(5, 5), (1, 1), (None, 0)]
            for frame, exp in zip(frames, expected, strict=True):
                assert frame.extra == exp


class TestYieldFrames:
    def test_main(self) -> None:
        def f() -> None:
            return g()

        def g() -> None:
            raise NotImplementedError

        with raises(NotImplementedError) as exc_info:
            f()
        frames = list(yield_frames(traceback=exc_info.tb))
        assert len(frames) == 3
        expected = ["test_main", "f", "g"]
        for frame, exp in zip(frames, expected, strict=True):
            assert frame.f_code.co_name == exp
