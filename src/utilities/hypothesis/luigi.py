from typing import Any

from beartype import beartype
from hypothesis.strategies import composite

from utilities.hypothesis import temp_paths


@composite
@beartype
def task_namespaces(_draw: Any, /) -> str:
    """Strategy for generating task namespaces."""
    path = _draw(temp_paths())
    return path.name
