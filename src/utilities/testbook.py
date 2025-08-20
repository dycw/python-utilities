from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from testbook import testbook

from utilities.pytest import throttle
from utilities.text import pascal_case

if TYPE_CHECKING:
    from collections.abc import Callable

    from utilities.types import Delta, PathLike


def build_notebook_tester(
    path: PathLike, /, *, throttle: Delta | None = None, on_try: bool = False
) -> type[Any]:
    """Build the notebook tester class."""
    path = Path(path)
    name = f"Test{pascal_case(path.stem)}"
    notebooks = [
        path_i
        for path_i in path.rglob("**/*.ipynb")
        if all(p != ".ipynb_checkpoints" for p in path_i.parts)
    ]
    namespace = {
        f"test_{p.stem.replace('-', '_')}": _build_test_method(
            p, delta=throttle, on_try=on_try
        )
        for p in notebooks
    }
    return type(name, (), namespace)


def _build_test_method(
    path: Path, /, *, delta: Delta | None = None, on_try: bool = False
) -> Callable[..., Any]:
    @testbook(path, execute=True)
    def method(self: Any, tb: Any) -> None:
        _ = (self, tb)  # pragma: no cover

    if delta is not None:
        method = throttle(delta=delta, on_try=on_try)(method)

    return method


__all__ = ["build_notebook_tester"]
