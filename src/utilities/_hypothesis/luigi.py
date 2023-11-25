from __future__ import annotations

from hypothesis.strategies import composite

from utilities._hypothesis.common import DrawFn, lift_draw, temp_paths


@composite
def namespace_mixins(_draw: DrawFn, /) -> type:
    """Strategy for generating task namespace mixins."""
    draw = lift_draw(_draw)
    path = draw(temp_paths())

    class NamespaceMixin:
        task_namespace = path.name

    return NamespaceMixin


__all__ = ["namespace_mixins"]
