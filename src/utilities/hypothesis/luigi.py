from hypothesis.strategies import DrawFn
from hypothesis.strategies import composite

from utilities.hypothesis import temp_paths


@composite
def task_namespaces(_draw: DrawFn, /) -> str:
    """Strategy for generating task namespaces."""

    path = _draw(temp_paths())
    return path.name
