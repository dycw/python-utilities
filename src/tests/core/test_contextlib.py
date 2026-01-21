from __future__ import annotations

from dataclasses import dataclass
from typing import override

from pytest import raises

from utilities.core import suppress_super_attribute_error


class TestSuppressSuperAttributeError:
    def test_main(self) -> None:
        inits: list[str] = []

        @dataclass(kw_only=True)
        class A:
            def __post_init__(self) -> None:
                with suppress_super_attribute_error():
                    super().__post_init__()  # pyright:ignore [reportAttributeAccessIssue]
                nonlocal inits
                inits.append("A")

        @dataclass(kw_only=True)
        class B: ...

        @dataclass(kw_only=True)
        class C:
            def __post_init__(self) -> None:
                with suppress_super_attribute_error():
                    super().__post_init__()  # pyright:ignore [reportAttributeAccessIssue]
                nonlocal inits
                inits.append("C")

        @dataclass(kw_only=True)
        class D: ...

        @dataclass(kw_only=True)
        class E(A, B, C, D):
            @override
            def __post_init__(self) -> None:
                super().__post_init__()
                nonlocal inits
                inits.append("E")

        _ = E()
        assert inits == ["C", "A", "E"]

    def test_error(self) -> None:
        @dataclass(kw_only=True)
        class Parent:
            def __post_init__(self) -> None:
                with suppress_super_attribute_error():
                    _ = self.error  # pyright:ignore [reportAttributeAccessIssue]

        @dataclass(kw_only=True)
        class Child(Parent):
            @override
            def __post_init__(self) -> None:
                super().__post_init__()

        with raises(AttributeError, match=r"'Child' object has no attribute 'error'"):
            _ = Child()
