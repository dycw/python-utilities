from __future__ import annotations

from collections.abc import Callable
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from functools import partial, wraps
from inspect import iscoroutinefunction
from typing import TYPE_CHECKING, Any, Literal, NoReturn, TypeVar, cast, overload

from treelib import Tree

from utilities.datetime import get_now
from utilities.functions import get_class_name
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Iterable
    from zoneinfo import ZoneInfo

    from treelib import Node

    from utilities.types import StrMapping

# types


_F = TypeVar("_F", bound=Callable[..., Any])
_T = TypeVar("_T")


@dataclass(kw_only=True, slots=True)
class _TracerData:
    trees: list[Tree] = field(default_factory=list)
    tree: Tree | None = None
    node: Node | None = None


# context vars


_TRACER_CONTEXT: ContextVar[_TracerData] = ContextVar(
    "_CURRENT_TRACER_NODE", default=_TracerData()
)


@dataclass(kw_only=True, slots=True)
class _NodeData:
    module: str
    qualname: str
    kwargs: StrMapping = field(default_factory=dict)
    start_time: dt.datetime
    end_time: dt.datetime | None = None
    outcome: Literal["success", "failure"] | None = None
    error: type[Exception] | None = None

    @property
    def desc(self) -> str:
        terms: list[Any] = []
        if (self.outcome == "failure") and (self.error is not None):
            terms.append(get_class_name(self.error))
        terms.append(self.duration)
        joined = ", ".join(map(str, terms))
        return f"{self.tag} ({joined})"

    @property
    def duration(self) -> dt.timedelta | None:
        return None if self.end_time is None else (self.end_time - self.start_time)

    @property
    def tag(self) -> str:
        return f"{self.module}:{self.qualname}"


@overload
def tracer(func: _F, /, *, time_zone: ZoneInfo | str = ...) -> _F: ...
@overload
def tracer(
    func: None = None, /, *, time_zone: ZoneInfo | str = ...
) -> Callable[[_F], _F]: ...
def tracer(
    func: _F | None = None, *, time_zone: ZoneInfo | str = UTC
) -> _F | Callable[[_F], _F]:
    """Context manager for tracing function calls."""
    if func is None:
        result = partial(tracer, time_zone=time_zone)
        return cast(Callable[[_F], _F], result)

    if iscoroutinefunction(func):

        @wraps(func)
        async def wrapped_async(*args: Any, **kwargs: Any) -> Any:
            node_data, tree, tracer_data, token = _initialize(
                func, time_zone=time_zone, **kwargs
            )
            try:
                result = await func(*args, **kwargs)
            except Exception as error:  # noqa: BLE001
                _handle_error(node_data, error)
            else:
                return _handle_success(node_data, result)
            finally:
                _cleanup(node_data, tracer_data, token, time_zone=time_zone, tree=tree)

        return cast(Any, wrapped_async)

    @wraps(func)
    def wrapped_sync(*args: Any, **kwargs: Any) -> Any:
        node_data, tree, tracer_data, token = _initialize(
            func, time_zone=time_zone, **kwargs
        )
        try:
            result = func(*args, **kwargs)
        except Exception as error:  # noqa: BLE001
            _handle_error(node_data, error)
        else:
            return _handle_success(node_data, result)
        finally:
            _cleanup(node_data, tracer_data, token, time_zone=time_zone, tree=tree)

    return cast(Any, wrapped_sync)


def get_tracer_trees() -> list[Tree]:
    """Get the tracer trees."""
    return _TRACER_CONTEXT.get().trees


def set_tracer_trees(trees: Iterable[Tree], /) -> None:
    """Set the tracer tree."""
    _ = _TRACER_CONTEXT.set(_TracerData(trees=list(trees)))


def _initialize(
    func: Callable[..., Any], /, *, time_zone: ZoneInfo | str = UTC, **kwargs: Any
) -> tuple[_NodeData, Tree | None, _TracerData, Token[_TracerData]]:
    node_data = _NodeData(
        module=func.__module__,
        qualname=func.__qualname__,
        kwargs=kwargs,
        start_time=get_now(time_zone=time_zone),
    )
    tracer_data: _TracerData = _TRACER_CONTEXT.get()
    if (tree := tracer_data.tree) is None:
        tree_use = tracer_data.tree = Tree()
        tracer_data.trees.append(tree_use)
    else:
        tree_use = tree
    parent_node = tracer_data.node
    child = tree_use.create_node(tag=node_data.tag, parent=parent_node, data=node_data)
    token = _TRACER_CONTEXT.set(
        _TracerData(trees=tracer_data.trees, tree=tree_use, node=child)
    )
    return node_data, tree, tracer_data, token


def _handle_error(node_data: _NodeData, error: Exception, /) -> NoReturn:
    node_data.outcome = "failure"
    node_data.error = type(error)
    raise error


def _handle_success(node_data: _NodeData, result: _T, /) -> _T:
    node_data.outcome = "success"
    return result


def _cleanup(
    node_data: _NodeData,
    tracer_data: _TracerData,
    token: Token[_TracerData],
    /,
    *,
    time_zone: ZoneInfo | str = UTC,
    tree: Tree | None = None,
) -> None:
    node_data.end_time = get_now(time_zone=time_zone)
    if tree is None:
        tracer_data.tree = None
    _TRACER_CONTEXT.reset(token)


__all__ = ["get_tracer_trees", "set_tracer_trees", "tracer"]
