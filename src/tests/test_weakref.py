from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import integers

from utilities.weakref import add_finalizer

if TYPE_CHECKING:
    from _pytest.capture import CaptureFixture


class TestAddFinalizer:
    @given(n=integers())
    @settings(suppress_health_check={HealthCheck.function_scoped_fixture})
    def test_main(self, *, n: int, capsys: CaptureFixture) -> None:
        @dataclass(frozen=True, kw_only=True)
        class Example:
            n: int

        obj = Example(n=n)

        def callback() -> None:
            print(f"Deleting n={n}...")  # noqa: T201

        out = capsys.readouterr().out
        assert out == ""
        _ = add_finalizer(obj, callback)
        del obj
        out = capsys.readouterr().out
        assert out == f"Deleting n={n}...\n"
