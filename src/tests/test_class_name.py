from utilities.class_name import get_class_name


class TestGetClassName:
    def test_class(self) -> None:
        class Example:
            ...

        result = get_class_name(Example)
        expected = "Example"
        assert result == expected

    def test_instance(self) -> None:
        class Example:
            ...

        result = get_class_name(Example())
        expected = "Example"
        assert result == expected
