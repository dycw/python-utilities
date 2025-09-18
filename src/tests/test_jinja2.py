from __future__ import annotations

from typing import TYPE_CHECKING

from jinja2 import DictLoader
from pytest import raises

from utilities.jinja2 import (
    EnhancedEnvironment,
    TemplateJob,
    _TemplateJobTargetDoesNotExistError,
    _TemplateJobTemplateDoesNotExistError,
)
from utilities.text import strip_and_dedent

if TYPE_CHECKING:
    from pathlib import Path


class TestEnhancedTemplate:
    def test_main(self) -> None:
        env = EnhancedEnvironment(
            loader=DictLoader({
                "test.j2": strip_and_dedent("""
                    text   = '{{ text }}'
                    kebab  = '{{ text | kebab }}'
                    pascal = '{{ text | pascal }}'
                    snake  = '{{ text | snake }}'
                """)
            })
        )
        result = env.get_template("test.j2").render(text="multi-word string")
        expected = strip_and_dedent("""
            text   = 'multi-word string'
            kebab  = 'multi-word-string'
            pascal = 'MultiWordString'
            snake  = 'multi_word_string'
        """)
        assert result == expected


class TestTemplateJob:
    def test_main(self, *, tmp_path: Path) -> None:
        path_template = tmp_path.joinpath("template.j2")
        _ = path_template.write_text(
            strip_and_dedent("""
                text = '{{ text }}'
            """)
        )
        path_target = tmp_path.joinpath("target.txt")
        job = TemplateJob(
            template=path_template, kwargs={"text": "example text"}, target=path_target
        )
        expected = strip_and_dedent("""
            text = 'example text'
        """)
        assert job.rendered == expected
        assert not path_target.exists()
        job.run()
        assert path_target.exists()
        assert path_target.read_text() == expected

    def test_append(self, *, tmp_path: Path) -> None:
        path_template = tmp_path.joinpath("template.j2")
        _ = path_template.write_text(
            strip_and_dedent(
                """
                new = '{{ text }}'
                """,
                trailing=True,
            )
        )
        path_target = tmp_path.joinpath("target.txt")
        _ = path_target.write_text(
            strip_and_dedent(
                """
                old = 'old text'
                """,
                trailing=True,
            )
        )
        job = TemplateJob(
            template=path_template,
            kwargs={"text": "new text"},
            target=path_target,
            mode="append",
        )
        job.run()
        assert path_target.exists()
        assert path_target.read_text() == strip_and_dedent("""
            old = 'old text'
            new = 'new text'
        """)

    def test_error_template(self, *, tmp_path: Path) -> None:
        path_template = tmp_path.joinpath("template.j2")
        path_target = tmp_path.joinpath("target.txt")
        with raises(
            _TemplateJobTemplateDoesNotExistError,
            match=r"^Template '.*' does not exist$",
        ):
            _ = TemplateJob(template=path_template, kwargs={}, target=path_target)

    def test_error_target(self, *, tmp_path: Path) -> None:
        path_template = tmp_path.joinpath("template.j2")
        path_template.touch()
        path_target = tmp_path.joinpath("target.txt")
        with raises(
            _TemplateJobTargetDoesNotExistError, match=r"^Target '.*' does not exist$"
        ):
            _ = TemplateJob(
                template=path_template, kwargs={}, target=path_target, mode="append"
            )
