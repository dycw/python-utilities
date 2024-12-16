from dataclasses import dataclass
from typing import Self

from utilities.beartype import beartype_cond


@dataclass(kw_only=True)
class Example_TestTypingFuncs_Outer:  # noqa: N801
    inner: "Example_TestTypingFuncs_Inner"


@dataclass(kw_only=True)
class Example_TestTypingFuncs_Inner:  # noqa: N801
    x: None = None


@dataclass(kw_only=True)
class Example_TestTypingFuncs_BeartypeCondOnMethod:  # noqa: N801
    @beartype_cond
    def func(self, x: "Example_TestTypingFuncs_BeartypeCondOnMethod") -> Self:
        _ = x
        return self
