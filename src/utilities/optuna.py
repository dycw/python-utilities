from collections.abc import Callable
from typing import TypeVar

from optuna import Study, Trial

from utilities.dataclasses import Dataclass

_T = TypeVar("_T", bound=Dataclass)


def get_best_params(study: Study, cls: type[_T], /) -> _T:
    """Get the best params as a dataclass."""
    return cls(**study.best_params)


def make_objective(
    suggest_params: Callable[[Trial], _T], objective: Callable[[_T], float], /
) -> Callable[[Trial], float]:
    """Make an objective given separate trialling & evaluating functions."""

    def inner(trial: Trial, /) -> float:
        return objective(suggest_params(trial))

    return inner


def suggest_bool(trial: Trial, name: str, /) -> bool:
    """Suggest a boolean value."""
    return trial.suggest_categorical(name, [True, False])


__all__ = ["get_best_params", "make_objective", "suggest_bool"]
