from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from optuna import create_study
from sqlalchemy import URL

from utilities.types import Dataclass

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from optuna import Study, Trial
    from optuna.pruners import BasePruner
    from optuna.samplers import BaseSampler
    from optuna.study import StudyDirection

    from utilities.types import PathLike


def create_sqlite_study(
    path: PathLike,
    /,
    *,
    sampler: BaseSampler | None = None,
    pruner: BasePruner | None = None,
    study_name: str | None = None,
    direction: str | StudyDirection | None = None,
    directions: Sequence[str | StudyDirection] | None = None,
) -> Study:
    """Create a study backed by SQLite."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    url = URL.create("sqlite", database=str(path))
    return create_study(
        storage=url.render_as_string(hide_password=False),
        sampler=sampler,
        pruner=pruner,
        study_name=study_name,
        direction=direction,
        load_if_exists=True,
        directions=directions,
    )


##


def get_best_params[T: Dataclass](study: Study, cls: type[T], /) -> T:
    """Get the best params as a dataclass."""
    return cls(**study.best_params)


##


def make_objective[T: Dataclass](
    suggest_params: Callable[[Trial], T], objective: Callable[[T], float], /
) -> Callable[[Trial], float]:
    """Make an objective given separate trialling & evaluating functions."""

    def inner(trial: Trial, /) -> float:
        return objective(suggest_params(trial))

    return inner


##


def suggest_bool(trial: Trial, name: str, /) -> bool:
    """Suggest a boolean value."""
    return trial.suggest_categorical(name, [True, False])


__all__ = ["create_sqlite_study", "get_best_params", "make_objective", "suggest_bool"]
