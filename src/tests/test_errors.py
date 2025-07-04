from __future__ import annotations

from asyncio import TaskGroup

from pytest import RaisesGroup, raises

from utilities.errors import ImpossibleCaseError, repr_error


class TestImpossibleCaseError:
    def test_main(self) -> None:
        x = None
        with raises(ImpossibleCaseError, match=r"Case must be possible: x=None\."):
            raise ImpossibleCaseError(case=[f"{x=}"])


class TestReprError:
    def test_class(self) -> None:
        class CustomError(Exception): ...

        result = repr_error(CustomError)
        expected = "CustomError"
        assert result == expected

    async def test_group(self) -> None:
        class Custom1Error(Exception): ...

        async def coroutine1() -> None:
            raise Custom1Error

        class Custom2Error(Exception): ...

        async def coroutine2() -> None:
            msg = "message2"
            raise Custom2Error(msg)

        with RaisesGroup(Custom1Error, Custom2Error) as exc_info:
            async with TaskGroup() as tg:
                _ = tg.create_task(coroutine1())
                _ = tg.create_task(coroutine2())
        result = repr_error(exc_info.value)
        expected = "ExceptionGroup(Custom1Error(), Custom2Error(message2))"
        assert result == expected

    def test_instance(self) -> None:
        class CustomError(Exception): ...

        result = repr_error(CustomError("message"))
        expected = "CustomError(message)"
        assert result == expected
