from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from utilities.types import Dataclass

if TYPE_CHECKING:
    from collections.abc import Callable

    from optuna import Study, Trial


_TDataclass = TypeVar("_TDataclass", bound=Dataclass)


def get_best_params(study: Study, cls: type[_TDataclass], /) -> _TDataclass:
    """Get the best params as a dataclass."""
    return cls(**study.best_params)


def make_objective(
    suggest_params: Callable[[Trial], _TDataclass],
    objective: Callable[[_TDataclass], float],
    /,
) -> Callable[[Trial], float]:
    """Make an objective given separate trialling & evaluating functions."""

    def inner(trial: Trial, /) -> float:
        return objective(suggest_params(trial))

    return inner


def suggest_bool(trial: Trial, name: str, /) -> bool:
    """Suggest a boolean value."""
    return trial.suggest_categorical(name, [True, False])


__all__ = ["get_best_params", "make_objective", "suggest_bool"]
