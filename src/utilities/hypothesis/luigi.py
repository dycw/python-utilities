from __future__ import annotations

from hypothesis.strategies import composite

from .hypothesis import DrawFn  # noqa: TID252
from .hypothesis import lift_draw  # noqa: TID252
from .hypothesis import temp_paths  # noqa: TID252


@composite
def namespace_mixins(_draw: DrawFn, /) -> type:
    """Strategy for generating task namespace mixins."""
    draw = lift_draw(_draw)
    path = draw(temp_paths())

    class NamespaceMixin:
        task_namespace = path.name

    return NamespaceMixin
