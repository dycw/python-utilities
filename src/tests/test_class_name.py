from pytest import mark, param

from utilities.class_name import get_class_name


class TestGetClassName:
    @mark.parametrize(
        ("snake", "expected"), [param(False, "Example"), param(True, "example")]
    )
    def test_class(self, snake: bool, expected: str) -> None:
        class Example:
            ...

        result = get_class_name(Example, snake=snake)
        assert result == expected

    @mark.parametrize(
        ("snake", "expected"), [param(False, "Example"), param(True, "example")]
    )
    def test_instance(self, snake: bool, expected: str) -> None:
        class Example:
            ...

        result = get_class_name(Example(), snake=snake)
        assert result == expected
