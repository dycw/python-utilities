from __future__ import annotations

from io import StringIO
from logging import StreamHandler, getLogger
from typing import TYPE_CHECKING, ClassVar, Literal

from pytest import raises

from tests.conftest import SKIPIF_CI
from tests.test_traceback_funcs.chain import func_chain_first
from tests.test_traceback_funcs.decorated_async import func_decorated_async_first
from tests.test_traceback_funcs.decorated_sync import func_decorated_sync_first
from tests.test_traceback_funcs.error_bind import (
    func_error_bind_async,
    func_error_bind_sync,
)
from tests.test_traceback_funcs.many import func_many
from tests.test_traceback_funcs.one import func_one
from tests.test_traceback_funcs.recursive import func_recursive
from tests.test_traceback_funcs.task_group_one import func_task_group_one_first
from tests.test_traceback_funcs.task_group_two import func_task_group_two_first
from tests.test_traceback_funcs.two import func_two_first
from tests.test_traceback_funcs.untraced import func_untraced
from utilities.errors import ImpossibleCaseError
from utilities.functions import ensure_str, is_sequence_of
from utilities.iterables import OneNonUniqueError, one
from utilities.text import strip_and_dedent
from utilities.traceback import (
    ExcChainTB,
    ExcGroupTB,
    ExcTB,
    RichTracebackFormatter,
    _CallArgsError,
    _format_exception,
    _Frame,
    get_rich_traceback,
    trace,
    yield_exceptions,
    yield_extended_frame_summaries,
    yield_frames,
)

if TYPE_CHECKING:
    from pathlib import Path
    from re import Pattern
    from traceback import FrameSummary
    from types import FrameType


class TestFormatException:
    def test_main(self) -> None:
        error = ValueError("Generic value error")
        result = _format_exception(error)
        expected = "builtins.ValueError(Generic value error)"
        assert result == expected

    def test_custom(self) -> None:
        x = 0
        error = ImpossibleCaseError(case=[f"{x=}"])
        result = _format_exception(error)
        expected = "utilities.errors.ImpossibleCaseError(Case must be possible: x=0.)"
        assert result == expected


class TestFrame:
    frame: ClassVar[_Frame] = _Frame(
        module="module",
        name="name",
        code_line="code_line",
        line_num=1,
        args=(1, 2, 3, 4),
        kwargs={"c": 5, "d": 6, "e": 7},
        locals={"a": 2, "b": 4, "args": (6, 8), "kwargs": {"d": 12, "e": 14}},
    )

    def test_repr(self) -> None:
        result = repr(self.frame)
        expected = strip_and_dedent("""
        Frame 1/1: name (module)
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
                args = (6, 8)
                kwargs = {'d': 12, 'e': 14}
            Line 1:
                code_line
        """)
        assert result == expected

    def test_summary(self) -> None:
        result = self.frame.format(detail=False)
        expected = "Frame 1/1: name (module)"
        assert result == expected


class TestGetRichTraceback:
    def test_func_one(self, *, traceback_func_one: Pattern[str]) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_one(1, 2, 3, 4, c=5, d=6, e=7)
        exc_tb = get_rich_traceback(exc_info.value)
        assert isinstance(exc_tb, ExcTB)
        assert len(exc_tb) == 1
        frame = exc_tb[0]  # to hit coverage
        assert frame.module == "tests.test_traceback_funcs.one"
        assert frame.name == "func_one"
        assert (
            frame.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert frame.args == (1, 2, 3, 4)
        assert frame.kwargs == {"c": 5, "d": 6, "e": 7}
        assert frame.locals["a"] == 2
        assert frame.locals["b"] == 4
        assert frame.locals["args"] == (6, 8)
        assert frame.locals["kwargs"] == {"d": 12, "e": 14}
        assert isinstance(exc_tb.error, AssertionError)

        assert traceback_func_one.search(repr(exc_tb))

    def test_func_two(self, *, traceback_func_two: Pattern[str]) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_two_first(1, 2, 3, 4, c=5, d=6, e=7)
        exc_tb = get_rich_traceback(exc_info.value)
        assert isinstance(exc_tb, ExcTB)
        assert len(exc_tb) == 2
        frame1, frame2 = exc_tb
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
        assert isinstance(exc_tb.error, AssertionError)

        assert traceback_func_two.search(repr(exc_tb))

    def test_func_chain(self, *, traceback_func_chain: Pattern[str]) -> None:
        with raises(ValueError, match=".*") as exc_info:
            _ = func_chain_first(1, 2, 3, 4, c=5, d=6, e=7)
        exc_chain_tb = get_rich_traceback(exc_info.value)
        assert isinstance(exc_chain_tb, ExcChainTB)
        assert len(exc_chain_tb) == 2
        for i in range(2):
            _ = exc_chain_tb[i]  # to hit coverage
        exc_chain_tb1, exc_chain_tb2 = exc_chain_tb
        assert isinstance(exc_chain_tb1, ExcTB)
        assert len(exc_chain_tb1) == 1
        frame1 = exc_chain_tb1[0]
        assert frame1.module == "tests.test_traceback_funcs.chain"
        assert frame1.name == "func_chain_first"
        assert frame1.code_line == "raise ValueError(msg) from error"
        assert frame1.args == (1, 2, 3, 4)
        assert frame1.kwargs == {"c": 5, "d": 6, "e": 7}
        assert frame1.locals["a"] == 2
        assert frame1.locals["b"] == 4
        assert frame1.locals["args"] == (6, 8)
        assert frame1.locals["kwargs"] == {"d": 12, "e": 14}
        assert isinstance(exc_chain_tb2, ExcTB)
        assert len(exc_chain_tb2) == 1
        frame2 = one(exc_chain_tb2)
        assert frame2.module == "tests.test_traceback_funcs.chain"
        assert frame2.name == "func_chain_second"
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

        assert traceback_func_chain.search(repr(exc_chain_tb))

    def test_func_decorated_sync(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_decorated_sync_first(1, 2, 3, 4, c=5, d=6, e=7)
        exc_tb = get_rich_traceback(exc_info.value)
        assert isinstance(exc_tb, ExcTB)
        self._assert_decorated(exc_tb, "sync")
        assert len(exc_tb) == 5

    @SKIPIF_CI
    async def test_func_decorated_async(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = await func_decorated_async_first(1, 2, 3, 4, c=5, d=6, e=7)
        exc_tb = get_rich_traceback(exc_info.value)
        assert isinstance(exc_tb, ExcTB)
        self._assert_decorated(exc_tb, "async")

    def test_func_many(
        self,
        *,
        traceback_func_many_long: Pattern[str],
        traceback_func_many_short: Pattern[str],
    ) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_many(1, 2, 3, 4, c=5, d=6, e=7)
        exc_tb_long = get_rich_traceback(exc_info.value)
        assert isinstance(exc_tb_long, ExcTB)
        assert traceback_func_many_long.search(repr(exc_tb_long))

        with raises(AssertionError) as exc_info:
            _ = func_many(1, 2, 3, 4, c=5, d=6, e=7)
        exc_tb_short = get_rich_traceback(exc_info.value, max_length=5)
        assert isinstance(exc_tb_short, ExcTB)
        assert traceback_func_many_short.search(repr(exc_tb_short))

    def test_func_recursive(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_recursive(1, 2, 3, 4, c=5, d=6, e=7)
        exc_tb = get_rich_traceback(exc_info.value)
        assert isinstance(exc_tb, ExcTB)
        assert len(exc_tb) == 2
        frame1, frame2 = exc_tb
        assert frame1.module == "tests.test_traceback_funcs.recursive"
        assert frame1.name == "func_recursive"
        assert frame1.code_line == "return func_recursive(a, b, *args, c=c, **kwargs)"
        assert frame1.args == (1, 2, 3, 4)
        assert frame1.kwargs == {"c": 5, "d": 6, "e": 7}
        assert frame1.locals["a"] == 2
        assert frame1.locals["b"] == 4
        assert frame1.locals["args"] == (6, 8)
        assert frame1.locals["kwargs"] == {"d": 12, "e": 14}
        assert frame2.module == "tests.test_traceback_funcs.recursive"
        assert frame2.name == "func_recursive"
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
        assert isinstance(exc_tb.error, AssertionError)

    @SKIPIF_CI
    async def test_func_task_group_one(
        self, *, traceback_func_task_group_one: Pattern[str]
    ) -> None:
        with raises(ExceptionGroup) as exc_info:
            await func_task_group_one_first(1, 2, 3, 4, c=5, d=6, e=7)
        exc_group_tb = get_rich_traceback(exc_info.value)
        assert isinstance(exc_group_tb, ExcGroupTB)
        assert isinstance(exc_group_tb.exc_group, ExcTB)
        assert len(exc_group_tb.exc_group) == 1
        frame_outer = one(exc_group_tb.exc_group)
        assert frame_outer.module == "tests.test_traceback_funcs.task_group_one"
        assert frame_outer.name == "func_task_group_one_first"
        assert frame_outer.code_line == "async with TaskGroup() as tg:"
        assert frame_outer.args == (1, 2, 3, 4)
        assert frame_outer.kwargs == {"c": 5, "d": 6, "e": 7}
        assert frame_outer.locals["a"] == 2
        assert frame_outer.locals["b"] == 4
        assert frame_outer.locals["args"] == (6, 8)
        assert frame_outer.locals["kwargs"] == {"d": 12, "e": 14}
        assert isinstance(exc_group_tb.exc_group.error, ExceptionGroup)
        assert len(exc_group_tb.exc_group.error.exceptions) == 1
        assert isinstance(one(exc_group_tb.exc_group.error.exceptions), AssertionError)
        assert len(exc_group_tb.errors) == 1
        assert is_sequence_of(exc_group_tb.errors, ExcTB)
        assert len(one(exc_group_tb.errors)) == 1
        frame_inner = one(one(exc_group_tb.errors))
        assert frame_inner.module == "tests.test_traceback_funcs.task_group_one"
        assert frame_inner.name == "func_task_group_one_second"
        assert (
            frame_inner.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert frame_inner.args == (2, 4, 6, 8)
        assert frame_inner.kwargs == {"c": 10, "d": 12, "e": 14}
        assert frame_inner.locals["a"] == 4
        assert frame_inner.locals["b"] == 8
        assert frame_inner.locals["args"] == (12, 16)
        assert frame_inner.locals["kwargs"] == {"d": 24, "e": 28}
        assert isinstance(one(exc_group_tb.errors).error, AssertionError)

        res_group = repr(exc_group_tb)
        assert traceback_func_task_group_one.search(res_group)

    @SKIPIF_CI
    async def test_func_task_group_two(self) -> None:
        with raises(ExceptionGroup) as exc_info:
            await func_task_group_two_first(1, 2, 3, 4, c=5, d=6, e=7)
        exc_group_tb = get_rich_traceback(exc_info.value)
        assert isinstance(exc_group_tb, ExcGroupTB)
        assert isinstance(exc_group_tb.exc_group, ExcTB)
        assert len(exc_group_tb.exc_group) == 1
        frame_outer = one(exc_group_tb.exc_group)
        assert frame_outer.module == "tests.test_traceback_funcs.task_group_two"
        assert frame_outer.name == "func_task_group_two_first"
        assert frame_outer.code_line == "async with TaskGroup() as tg:"
        assert frame_outer.args == (1, 2, 3, 4)
        assert frame_outer.kwargs == {"c": 5, "d": 6, "e": 7}
        assert frame_outer.locals["a"] == 2
        assert frame_outer.locals["b"] == 4
        assert frame_outer.locals["args"] == (6, 8)
        assert frame_outer.locals["kwargs"] == {"d": 12, "e": 14}
        assert isinstance(exc_group_tb.exc_group.error, ExceptionGroup)
        assert len(exc_group_tb.errors) == 2
        assert is_sequence_of(exc_group_tb.exc_group.error.exceptions, AssertionError)
        assert is_sequence_of(exc_group_tb.errors, ExcTB)
        assert len(exc_group_tb.errors) == 2
        assert len(exc_group_tb.errors[0]) == 1
        frame_inner1 = one(exc_group_tb.errors[0])
        assert frame_inner1.module == "tests.test_traceback_funcs.task_group_two"
        assert frame_inner1.name == "func_task_group_two_second"
        assert (
            frame_inner1.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert frame_inner1.args == (2, 4, 6, 8)
        assert frame_inner1.kwargs == {"c": 10, "d": 12, "e": 14}
        assert frame_inner1.locals["a"] == 4
        assert frame_inner1.locals["b"] == 8
        assert frame_inner1.locals["args"] == (12, 16)
        assert frame_inner1.locals["kwargs"] == {"d": 24, "e": 28}
        assert isinstance(exc_group_tb.errors[0].error, AssertionError)
        assert len(exc_group_tb.errors[1]) == 1
        frame_inner2 = one(exc_group_tb.errors[1])
        assert frame_inner2.module == "tests.test_traceback_funcs.task_group_two"
        assert frame_inner2.name == "func_task_group_two_second"
        assert (
            frame_inner2.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert frame_inner2.args == (3, 5, 7, 9)
        assert frame_inner2.kwargs == {"c": 11, "d": 13, "e": 15}
        assert frame_inner2.locals["a"] == 6
        assert frame_inner2.locals["b"] == 10
        assert frame_inner2.locals["args"] == (14, 18)
        assert frame_inner2.locals["kwargs"] == {"d": 26, "e": 30}
        assert isinstance(exc_group_tb.errors[1].error, AssertionError)

    def test_func_untraced(self) -> None:
        with raises(AssertionError) as exc_info:
            _ = func_untraced(1, 2, 3, 4, c=5, d=6, e=7)
        error = get_rich_traceback(exc_info.value)
        assert isinstance(error, AssertionError)

    def test_custom_error(self) -> None:
        @trace
        def raises_custom_error() -> bool:
            return one([True, False])

        with raises(OneNonUniqueError) as exc_info:
            _ = raises_custom_error()
        exc_path = get_rich_traceback(exc_info.value)
        assert isinstance(exc_path, ExcTB)
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
        self, exc_path: ExcTB, sync_or_async: Literal["sync", "async"], /
    ) -> None:
        assert len(exc_path) == 5
        frame1, frame2, _, frame4, frame5 = exc_path
        match sync_or_async:
            case "sync":
                maybe_await = ""
            case "async":
                maybe_await = "await "
        assert frame1.module == f"tests.test_traceback_funcs.decorated_{sync_or_async}"
        assert frame1.name == f"func_decorated_{sync_or_async}_first"
        assert (
            frame1.code_line
            == f"return {maybe_await}func_decorated_{sync_or_async}_second(a, b, *args, c=c, **kwargs)"
        )
        assert frame1.args == (1, 2, 3, 4)
        assert frame1.kwargs == {"c": 5, "d": 6, "e": 7}
        assert frame1.locals["a"] == 2
        assert frame1.locals["b"] == 4
        assert frame1.locals["args"] == (6, 8)
        assert frame1.locals["kwargs"] == {"d": 12, "e": 14}
        assert frame2.module == f"tests.test_traceback_funcs.decorated_{sync_or_async}"
        assert frame2.name == f"func_decorated_{sync_or_async}_second"
        assert (
            frame2.code_line
            == f"return {maybe_await}func_decorated_{sync_or_async}_third(a, b, *args, c=c, **kwargs)"
        )
        assert frame2.args == (2, 4, 6, 8)
        assert frame2.kwargs == {"c": 10, "d": 12, "e": 14}
        assert frame2.locals["a"] == 4
        assert frame2.locals["b"] == 8
        assert frame2.locals["args"] == (12, 16)
        assert frame2.locals["kwargs"] == {"d": 24, "e": 28}
        assert frame4.module == f"tests.test_traceback_funcs.decorated_{sync_or_async}"
        assert frame4.name == f"func_decorated_{sync_or_async}_fourth"
        assert (
            frame4.code_line
            == f"return {maybe_await}func_decorated_{sync_or_async}_fifth(a, b, *args, c=c, **kwargs)"
        )
        assert frame4.args == (8, 16, 24, 32)
        assert frame4.kwargs == {"c": 40, "d": 48, "e": 56}
        assert frame4.locals["a"] == 16
        assert frame4.locals["b"] == 32
        assert frame4.locals["args"] == (48, 64)
        assert frame4.locals["kwargs"] == {"d": 96, "e": 112}
        assert frame5.module == f"tests.test_traceback_funcs.decorated_{sync_or_async}"
        assert frame5.name == f"func_decorated_{sync_or_async}_fifth"
        assert (
            frame5.code_line
            == 'assert result % 10 == 0, f"Result ({result}) must be divisible by 10"'
        )
        assert frame5.args == (16, 32, 48, 64)
        assert frame5.kwargs == {"c": 80, "d": 96, "e": 112}
        assert frame5.locals["a"] == 32
        assert frame5.locals["b"] == 64
        assert frame5.locals["args"] == (96, 128)
        assert frame5.locals["kwargs"] == {"d": 192, "e": 224}
        assert isinstance(exc_path.error, AssertionError)


class TestRichTracebackFormatter:
    def test_decorated(
        self, *, tmp_path: Path, traceback_func_one: Pattern[str]
    ) -> None:
        logger = getLogger(str(tmp_path))
        logger.addHandler(handler := StreamHandler(buffer := StringIO()))
        handler.setFormatter(RichTracebackFormatter(detail=True))
        try:
            _ = func_one(1, 2, 3, 4, c=5, d=6, e=7)
        except AssertionError:
            logger.exception("message")
        result = buffer.getvalue()
        assert traceback_func_one.search(result)

    def test_undecorated(
        self, *, tmp_path: Path, traceback_func_untraced: Pattern[str]
    ) -> None:
        logger = getLogger(str(tmp_path))
        logger.addHandler(handler := StreamHandler(buffer := StringIO()))
        handler.setFormatter(RichTracebackFormatter(detail=True))
        try:
            _ = func_untraced(1, 2, 3, 4, c=5, d=6, e=7)
        except AssertionError:
            logger.exception("message")
        result = buffer.getvalue()
        assert traceback_func_untraced.search(result)

    def test_create_and_set(self) -> None:
        handler = StreamHandler()
        assert len(handler.filters) == 0
        _ = RichTracebackFormatter.create_and_set(handler)
        assert len(handler.filters) == 1

    def test_no_logging(self, *, tmp_path: Path) -> None:
        logger = getLogger(str(tmp_path))
        logger.addHandler(handler := StreamHandler(buffer := StringIO()))
        handler.setFormatter(RichTracebackFormatter(detail=True))
        logger.error("message")
        result, _ = buffer.getvalue().splitlines()
        expected = "ERROR: record.exc_info is None"
        assert result == expected

    def test_post(self, *, tmp_path: Path) -> None:
        logger = getLogger(str(tmp_path))
        logger.addHandler(handler := StreamHandler(buffer := StringIO()))
        handler.setFormatter(
            RichTracebackFormatter(detail=True, post=lambda x: f"> {x}")
        )
        try:
            _ = func_one(1, 2, 3, 4, c=5, d=6, e=7)
        except AssertionError:
            logger.exception("message")
        result = buffer.getvalue()
        assert result.startswith("> ")


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
