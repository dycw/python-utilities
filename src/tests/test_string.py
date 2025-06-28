from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from hypothesis import given

from utilities.hypothesis import temp_paths, text_ascii
from utilities.os import temp_environ
from utilities.string import substitute_environ
from utilities.text import strip_and_dedent

if TYPE_CHECKING:
    from pathlib import Path


class TestSubstituteEnviron:
    template: ClassVar[str] = strip_and_dedent("""
        This is a template string with:
         - key = '$TEMPLATE_KEY'
         - value = '$TEMPLATE_VALUE'
    """)

    @given(root=temp_paths(), key=text_ascii(), value=text_ascii())
    def test_file(self, *, root: Path, key: str, value: str) -> None:
        path = root.joinpath("file.txt")
        _ = path.write_text(self.template)
        self._run_test(path, key, value)

    @given(key=text_ascii(), value=text_ascii())
    def test_text_environ(self, *, key: str, value: str) -> None:
        self._run_test(self.template, key, value)

    @given(key=text_ascii(), value=text_ascii())
    def test_text_kwargs(self, *, key: str, value: str) -> None:
        result = substitute_environ(
            self.template, TEMPLATE_KEY=key, TEMPLATE_VALUE=value
        )
        self._assert_equal(result, key, value)

    def _run_test(self, path_or_text: Path | str, key: str, value: str) -> None:
        with temp_environ(TEMPLATE_KEY=key, TEMPLATE_VALUE=value):
            result = substitute_environ(path_or_text)
        self._assert_equal(result, key, value)

    def _assert_equal(self, text: str, key: str, value: str) -> None:
        expected = strip_and_dedent(f"""
            This is a template string with:
             - key = {key!r}
             - value = {value!r}
        """)
        assert text == expected
