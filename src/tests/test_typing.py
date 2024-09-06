from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, NamedTuple

from pytest import mark, param

from utilities.typing import (
    get_args,
    is_dict_type,
    is_frozenset_type,
    is_list_type,
    is_literal_type,
    is_mapping_type,
    is_namedtuple_class,
    is_namedtuple_instance,
    is_optional_type,
    is_sequence_type,
    is_set_type,
    is_union_type,
)

if TYPE_CHECKING:
    from collections.abc import Callable


class TestGetArgs:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(dict[int, int], (int, int)),
            param(frozenset[int], (int,)),
            param(int | None, (int,)),
            param(int | str, (int, str)),
            param(list[int], (int,)),
            param(Literal["a", "b", "c"], ("a", "b", "c")),
            param(Mapping[int, int], (int, int)),
            param(Sequence[int], (int,)),
            param(set[int], (int,)),
        ],
    )
    def test_main(self, *, obj: Any, expected: tuple[Any, ...]) -> None:
        result = get_args(obj)
        assert result == expected


class TestIsAnnotationOfType:
    @mark.parametrize(
        ("func", "obj", "expected"),
        [
            param(is_dict_type, dict[int, int], True),
            param(is_dict_type, list[int], False),
            param(is_frozenset_type, frozenset[int], True),
            param(is_frozenset_type, list[int], False),
            param(is_list_type, list[int], True),
            param(is_list_type, set[int], False),
            param(is_mapping_type, Mapping[int, int], True),
            param(is_mapping_type, list[int], False),
            param(is_literal_type, Literal["a", "b", "c"], True),
            param(is_literal_type, list[int], False),
            param(is_optional_type, int | None, True),
            param(is_optional_type, int | str, False),
            param(is_optional_type, list[int], False),
            param(is_sequence_type, Sequence[int], True),
            param(is_sequence_type, list[int], False),
            param(is_set_type, list[int], False),
            param(is_union_type, int | str, True),
            param(is_union_type, list[int], False),
        ],
    )
    def test_main(
        self, *, func: Callable[[Any], bool], obj: Any, expected: bool
    ) -> None:
        assert func(obj) is expected


class TestIsNamedTuple:
    def test_main(self) -> None:
        class Example(NamedTuple):
            x: int

        assert is_namedtuple_class(Example)
        assert is_namedtuple_instance(Example(x=0))

    def test_class(self) -> None:
        @dataclass(kw_only=True)
        class Example:
            x: int

        assert not is_namedtuple_class(Example)
        assert not is_namedtuple_instance(Example(x=0))
