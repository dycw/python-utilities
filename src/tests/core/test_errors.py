from __future__ import annotations

from asyncio import TaskGroup
from subprocess import CalledProcessError

from pytest import RaisesGroup

from utilities.core import async_sleep, normalize_multi_line_str, repr_error


class TestReprError:
    def test_class(self) -> None:
        class CustomError(Exception): ...

        result = repr_error(CustomError)
        expected = "CustomError"
        assert result == expected

    def test_called_process(self) -> None:
        error = CalledProcessError(1, "cmd", "stdout", "stderr")
        result = repr_error(error)
        expected = normalize_multi_line_str("""
            CalledProcessError(
                returncode │ 1
                cmd        │ cmd
                stdout     │ stdout
                stderr     │ stderr
            )
        """)
        assert result == expected

    def test_called_process_long_cmd(self) -> None:
        error = CalledProcessError(
            1, " ".join(["cmd", *(f"arg{i}" for i in range(20))]), "stdout", "stderr"
        )
        result = repr_error(error)
        expected = normalize_multi_line_str("""
            CalledProcessError(
                returncode │ 1
                cmd        │ cmd arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11
                           │ arg12 arg13 arg14 arg15 arg16 arg17 arg18 arg19
                stdout     │ stdout
                stderr     │ stderr
            )
        """)
        assert result == expected

    async def test_group(self) -> None:
        class Custom1Error(Exception): ...

        async def coroutine1() -> None:
            await async_sleep()
            raise Custom1Error

        class Custom2Error(Exception): ...

        async def coroutine2() -> None:
            await async_sleep()
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
