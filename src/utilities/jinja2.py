from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, assert_never, override

from jinja2 import BaseLoader, BytecodeCache, Environment, FileSystemLoader, Undefined
from jinja2.defaults import (
    BLOCK_END_STRING,
    BLOCK_START_STRING,
    COMMENT_END_STRING,
    COMMENT_START_STRING,
    LINE_COMMENT_PREFIX,
    LINE_STATEMENT_PREFIX,
    LSTRIP_BLOCKS,
    NEWLINE_SEQUENCE,
    TRIM_BLOCKS,
    VARIABLE_END_STRING,
    VARIABLE_START_STRING,
)

from utilities.core import kebab_case, pascal_case, snake_case, write_text

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path

    from jinja2.ext import Extension

    from utilities.types import StrMapping


class EnhancedEnvironment(Environment):
    """Environment with enhanced features."""

    @override
    def __init__(
        self,
        block_start_string: str = BLOCK_START_STRING,
        block_end_string: str = BLOCK_END_STRING,
        variable_start_string: str = VARIABLE_START_STRING,
        variable_end_string: str = VARIABLE_END_STRING,
        comment_start_string: str = COMMENT_START_STRING,
        comment_end_string: str = COMMENT_END_STRING,
        line_statement_prefix: str | None = LINE_STATEMENT_PREFIX,
        line_comment_prefix: str | None = LINE_COMMENT_PREFIX,
        trim_blocks: bool = TRIM_BLOCKS,
        lstrip_blocks: bool = LSTRIP_BLOCKS,
        newline_sequence: Literal["\n", "\r\n", "\r"] = NEWLINE_SEQUENCE,
        keep_trailing_newline: bool = True,
        extensions: Sequence[str | type[Extension]] = (),
        optimized: bool = True,
        undefined: type[Undefined] = Undefined,
        finalize: Callable[..., Any] | None = None,
        autoescape: bool | Callable[[str | None], bool] = False,
        loader: BaseLoader | None = None,
        cache_size: int = 400,
        auto_reload: bool = True,
        bytecode_cache: BytecodeCache | None = None,
        enable_async: bool = False,
    ) -> None:
        super().__init__(
            block_start_string,
            block_end_string,
            variable_start_string,
            variable_end_string,
            comment_start_string,
            comment_end_string,
            line_statement_prefix,
            line_comment_prefix,
            trim_blocks,
            lstrip_blocks,
            newline_sequence,
            keep_trailing_newline,
            extensions,
            optimized,
            undefined,
            finalize,
            autoescape,
            loader,
            cache_size,
            auto_reload,
            bytecode_cache,
            enable_async,
        )
        self.filters["kebab"] = kebab_case
        self.filters["pascal"] = pascal_case
        self.filters["snake"] = snake_case


@dataclass(order=True, unsafe_hash=True, kw_only=True, slots=True)
class TemplateJob:
    """A template with an associated rendering job."""

    template: Path
    kwargs: StrMapping
    target: Path
    mode: Literal["write", "append"] = "write"

    def __post_init__(self) -> None:
        if not self.template.exists():
            raise _TemplateJobTemplateDoesNotExistError(path=self.template)
        if (self.mode == "append") and not self.target.exists():
            raise _TemplateJobTargetDoesNotExistError(path=self.template)

    def run(self) -> None:
        """Run the job."""
        match self.mode:
            case "write":
                write_text(self.target, self.rendered, overwrite=True)
            case "append":
                with self.target.open(mode="a") as fh:
                    _ = fh.write(self.rendered)
            case never:
                assert_never(never)

    @property
    def rendered(self) -> str:
        """The template, rendered."""
        env = EnhancedEnvironment(loader=FileSystemLoader(self.template.parent))
        return env.get_template(self.template.name).render(self.kwargs)


@dataclass(kw_only=True, slots=True)
class TemplateJobError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _TemplateJobTemplateDoesNotExistError(TemplateJobError):
    path: Path

    @override
    def __str__(self) -> str:
        return f"Template {str(self.path)!r} does not exist"


@dataclass(kw_only=True, slots=True)
class _TemplateJobTargetDoesNotExistError(TemplateJobError):
    path: Path

    @override
    def __str__(self) -> str:
        return f"Target {str(self.path)!r} does not exist"


__all__ = ["EnhancedEnvironment", "TemplateJob", "TemplateJobError"]
