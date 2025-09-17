from __future__ import annotations

from jinja2 import DictLoader

from utilities.jinja2 import EnhancedEnvironment
from utilities.text import strip_and_dedent


class TestEnhancedTemplate:
    def test_main(self) -> None:
        env = EnhancedEnvironment(
            loader=DictLoader({
                "test.j2": strip_and_dedent("""
                    text   = '{{ text }}'
                    pascal = '{{ text | pascal }}'
                    snake  = '{{ text | snake }}'
                """)
            })
        )
        result = env.get_template("test.j2").render(text="multi-word string")
        expected = strip_and_dedent("""
            text   = 'multi-word string'
            pascal = 'MultiWordString'
            snake  = 'multi_word_string'
        """)
        assert result == expected
