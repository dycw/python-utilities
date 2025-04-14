from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from optuna import Trial, create_study
from pytest import approx

from utilities.optuna import (
    create_sqlite_study,
    get_best_params,
    make_objective,
    suggest_bool,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestCreateSQLiteStudy:
    def test_main(self, *, tmp_path: Path) -> None:
        path = tmp_path.joinpath("dir", "db.sqlite")
        _ = create_sqlite_study(path)
        assert path.is_file()


class TestGetBestParams:
    def test_main(self) -> None:
        def objective(trial: Trial, /) -> float:
            x = trial.suggest_float("x", 0.0, 4.0)
            return (x - 2.0) ** 2

        study = create_study(direction="minimize")
        study.optimize(objective, n_trials=200)

        @dataclass(kw_only=True, slots=True)
        class Params:
            x: float

        params = get_best_params(study, Params)
        assert params.x == approx(2.0, abs=1e-2)
        assert study.best_value == approx(0.0, abs=1e-4)


class TestMakeObjective:
    def test_main(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Params:
            x: float

        def suggest_params(trial: Trial, /) -> Params:
            return Params(x=trial.suggest_float("x", 0.0, 4.0))

        def objective(params: Params, /) -> float:
            return (params.x - 2.0) ** 2

        study = create_study(direction="minimize")
        study.optimize(make_objective(suggest_params, objective), n_trials=200)
        assert study.best_params["x"] == approx(2.0, abs=1e-2)
        assert study.best_value == approx(0.0, abs=1e-4)


class TestSuggestBool:
    def test_main(self) -> None:
        def objective(trial: Trial, /) -> float:
            x = trial.suggest_float("x", 0.0, 4.0)
            y = suggest_bool(trial, "y")
            return (x - 2.0) ** 2 + int(y)

        study = create_study(direction="minimize")
        study.optimize(objective, n_trials=200)
        params = study.best_params
        assert set(params) == {"x", "y"}
        assert params["x"] == approx(2.0, abs=1e-2)
        assert not params["y"]
        assert study.best_value == approx(0.0, abs=1e-4)
