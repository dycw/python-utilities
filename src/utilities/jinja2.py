from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, override

from jinja2 import BaseLoader, BytecodeCache, Environment, Undefined
from jinja2.defaults import (
    BLOCK_END_STRING,
    BLOCK_START_STRING,
    COMMENT_END_STRING,
    COMMENT_START_STRING,
    KEEP_TRAILING_NEWLINE,
    LINE_COMMENT_PREFIX,
    LINE_STATEMENT_PREFIX,
    LSTRIP_BLOCKS,
    NEWLINE_SEQUENCE,
    TRIM_BLOCKS,
    VARIABLE_END_STRING,
    VARIABLE_START_STRING,
)

from utilities.text import pascal_case, snake_case

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from jinja2.ext import Extension


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
        keep_trailing_newline: bool = KEEP_TRAILING_NEWLINE,
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
        self.filters["snake"] = snake_case
        self.filters["pascal"] = pascal_case


__all__ = ["EnhancedEnvironment"]
