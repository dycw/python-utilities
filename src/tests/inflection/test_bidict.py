from hypothesis import given
from pytest import raises

from utilities.hypothesis import text_ascii
from utilities.inflection import snake_case
from utilities.inflection.bidict import SnakeCaseContainsDuplicatesError
from utilities.inflection.bidict import snake_case_mappings


class TestSnakeCaseMappings:
    @given(text=text_ascii())
    def test_success(self, text: str) -> None:
        result = snake_case_mappings([text])
        expected = {text: snake_case(text)}
        assert result == expected

    @given(text=text_ascii(min_size=1))
    def test_error(self, text: str) -> None:
        with raises(SnakeCaseContainsDuplicatesError):
            _ = snake_case_mappings([text.lower(), text.upper()])
