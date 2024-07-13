from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Literal

from pytest import mark, param, raises

from utilities.typing import (
    GetLiteralArgsError,
    get_literal_args,
    is_dict_type,
    is_frozenset_type,
    is_list_type,
    is_literal_type,
    is_mapping_type,
    is_optional_type,
    is_set_type,
    is_union_type,
)

if TYPE_CHECKING:
    from collections.abc import Callable


class TestGetLiteralArgs:
    def test_main(self) -> None:
        literal = Literal["a", "b", "c"]
        result: tuple[Literal["a", "b", "c"], ...] = get_literal_args(literal)
        expected = ("a", "b", "c")
        assert result == expected

    def test_error(self) -> None:
        union = int | str
        with raises(
            GetLiteralArgsError,
            match="Object .* must be a Literal annotation; got int | str instead",
        ):
            _ = get_literal_args(union)


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
            param(is_set_type, set[int], True),
            param(is_set_type, list[int], False),
            param(is_union_type, int | str, True),
            param(is_union_type, list[int], False),
        ],
    )
    def test_main(
        self, *, func: Callable[[Any], bool], obj: Any, expected: bool
    ) -> None:
        assert func(obj) is expected
