from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from utilities.os import temp_environ
from utilities.string import substitute
from utilities.text import strip_and_dedent, unique_str

if TYPE_CHECKING:
    from pathlib import Path


class TestSubstitute:
    template: ClassVar[str] = strip_and_dedent("""
        This is a template string with:
         - key   = '$TEMPLATE_KEY'
         - value = '$TEMPLATE_VALUE'
    """)

    def test_path(self, *, temp_file: Path) -> None:
        _ = temp_file.write_text(self.template)
        key, value = unique_str(), unique_str()
        result = substitute(temp_file, TEMPLATE_KEY=key, TEMPLATE_VALUE=value)
        self._assert_equal(result, key, value)

    def test_text(self) -> None:
        key, value = unique_str(), unique_str()
        result = substitute(self.template, TEMPLATE_KEY=key, TEMPLATE_VALUE=value)
        self._assert_equal(result, key, value)

    def test_environ(self) -> None:
        key, value = unique_str(), unique_str()
        with temp_environ(TEMPLATE_KEY=key, TEMPLATE_VALUE=value):
            result = substitute(self.template, environ=True, key=key, value=value)
        self._assert_equal(result, key, value)

    def test_mapping(self) -> None:
        key, value = unique_str(), unique_str()
        result = substitute(
            self.template, mapping={"TEMPLATE_KEY": key, "TEMPLATE_VALUE": value}
        )
        self._assert_equal(result, key, value)

    def _assert_equal(self, text: str, key: str, value: str) -> None:
        expected = strip_and_dedent(f"""
            This is a template string with:
             - key   = {key!r}
             - value = {value!r}
        """)
        assert text == expected
