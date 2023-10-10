from __future__ import annotations

from hypothesis.strategies import composite

from utilities.hypothesis import DrawFn
from utilities.hypothesis import lift_draw
from utilities.hypothesis import temp_paths


@composite
def namespace_mixins(_draw: DrawFn, /) -> type:
    """Strategy for generating task namespace mixins."""
    draw = lift_draw(_draw)
    path = draw(temp_paths())

    class NamespaceMixin:
        task_namespace = path.name

    return NamespaceMixin
