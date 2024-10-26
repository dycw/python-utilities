from __future__ import annotations

import contextvars
import time
from contextlib import contextmanager

from treelib import Node, Tree

# Context variable to store the current node id for nested calls
current_node_id = contextvars.ContextVar("current_node_id", default=None)


class TimingNode:
    def __init__(self, name: str) -> None:
        self.name = name
        self.start_time = time.perf_counter()
        self.end_time = None

    def stop(self) -> None:
        self.end_time = time.perf_counter()

    @property
    def duration(self):
        return self.end_time - self.start_time if self.end_time else None

    def __lt__(self, other):
        return self.start_time < other.start_time

    def __repr__(self) -> str:
        duration = self.duration if self.end_time else 0.0
        return f"{self.name}: {duration:.4f}s"


# Initialize the tree
timing_tree = Tree()


def example_function() -> None:
    with timed("example_function"):
        inner_function()
        another_inner_function()
    print("Timing breakdown:")
    timing_tree.show()


def inner_function() -> None:
    with timed("inner_function"):
        time.sleep(0.2)


def another_inner_function() -> None:
    with timed("another_inner_function"):
        nested_function()


def nested_function() -> None:
    with timed("nested_function"):
        time.sleep(0.1)


class TestTracer:
    def test_main(self) -> None:
        example_function()
