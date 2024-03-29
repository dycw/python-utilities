from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest
from optuna import create_study

from utilities.optuna import get_best_params, make_objective, suggest_bool

if TYPE_CHECKING:
    from optuna import Trial


class TestGetBestParams:
    def test_main(self) -> None:
        def objective(trial: Trial, /) -> float:
            x = trial.suggest_float("x", 0.0, 4.0)
            return (x - 2.0) ** 2

        study = create_study(direction="minimize")
        study.optimize(objective, n_trials=100)

        @dataclass(frozen=True, kw_only=True)
        class Params:
            x: float

        params = get_best_params(study, Params)
        assert params.x == pytest.approx(2.0, abs=1e-2)
        assert study.best_value == pytest.approx(0.0, abs=1e-4)


class TestMakeObjective:
    def test_main(self) -> None:
        @dataclass(frozen=True, kw_only=True)
        class Params:
            x: float

        def suggest_params(trial: Trial, /) -> Params:
            return Params(x=trial.suggest_float("x", 0.0, 4.0))

        def objective(params: Params, /) -> float:
            return (params.x - 2.0) ** 2

        study = create_study(direction="minimize")
        study.optimize(make_objective(suggest_params, objective), n_trials=100)
        assert study.best_params["x"] == pytest.approx(2.0, abs=1e-2)
        assert study.best_value == pytest.approx(0.0, abs=1e-4)


class TestSuggestBool:
    def test_main(self) -> None:
        def objective(trial: Trial, /) -> float:
            x = trial.suggest_float("x", 0.0, 4.0)
            y = suggest_bool(trial, "y")
            return (x - 2.0) ** 2 + int(y)

        study = create_study(direction="minimize")
        study.optimize(objective, n_trials=100)
        params = study.best_params
        assert set(params) == {"x", "y"}
        assert params["x"] == pytest.approx(2.0, abs=1e-2)
        assert not params["y"]
        assert study.best_value == pytest.approx(0.0, abs=1e-4)
