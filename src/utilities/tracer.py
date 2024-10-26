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


@contextmanager
def timed(name: str):
    # Initialize a timing node for this call
    node = TimingNode(name)

    # Determine the parent node and establish the node in the tree
    parent_id = current_node_id.get()
    node_id = f"{id(node)}-{name}"  # Unique id for the node
    timing_tree.create_node(tag=node, identifier=node_id, parent=parent_id)

    # Set this node as the current context
    token = current_node_id.set(node_id)

    try:
        yield node
    finally:
        node.stop()
        current_node_id.reset(token)


