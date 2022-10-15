from pathlib import Path

from beartype import beartype
from hypothesis.strategies import SearchStrategy

from utilities.hypothesis import draw_and_map
from utilities.hypothesis import temp_paths


@beartype
def task_namespaces() -> SearchStrategy[str]:
    """Strategy for generating task namespaces."""

    return draw_and_map(_draw_task_namespace, temp_paths())


@beartype
def _draw_task_namespace(path: Path, /) -> str:
    return path.name
