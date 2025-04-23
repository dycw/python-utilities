from __future__ import annotations

from hypothesis import given
from hypothesis.strategies import sampled_from
from numpy import isclose
from polars import DataFrame, Float64, Series, Struct, col
from polars.testing import assert_frame_equal
from sklearn.linear_model import LinearRegression

from utilities.polars import concat_series, integers, normal
from utilities.polars_ols import compute_rolling_ols


class TestComputeRollingOLS:
    def test_main_self(self) -> None:
        df = self._df.with_columns(
            compute_rolling_ols(
                "y", "x1", "x2", window_size=5, min_periods=5, add_intercept=True
            )
        )
        self._assert_series(df["ols"])

    def test_main_series(self) -> None:
        ols = compute_rolling_ols(
            self._df["y"],
            self._df["x1"],
            self._df["x2"],
            window_size=5,
            min_periods=5,
            add_intercept=True,
        )
        self._assert_series(ols)

    @given(
        case=sampled_from([
            (
                slice(-7, -2),
                [0.3619563208480195, 0.6583229512154678],
                -1.386023798329262,
                7.434533329394103,
                0.994253238813284,
            ),
            (
                slice(-6, -1),
                [0.35564162435283264, 0.6656931556738643],
                -0.5626805730005437,
                -51.903154626050124,
                0.9979752966843768,
            ),
            (
                slice(-5, None),
                [0.3100421300754358, 0.6753578168818635],
                0.48493124625502837,
                -36.70039604095908,
                0.9977272526713715,
            ),
        ])
    )
    def test_tail(
        self, *, case: tuple[slice, list[float], float, float, float]
    ) -> None:
        slice_, coeffs, intercept, prediction, r2 = case
        df = self._df[slice_]
        X = df.select("x1", "x2").to_numpy()  # noqa: N806
        y = df.select("y").to_numpy()
        model = LinearRegression()
        model = model.fit(X, y)
        assert isclose(model.coef_, coeffs).all()
        assert isclose(model.intercept_, intercept)
        assert isclose(model.predict(X)[-1], prediction).all()
        assert isclose(model.score(X, y), r2)

    @property
    def _df(self) -> DataFrame:
        n = 20
        return concat_series(
            integers(n, -100, high=100, seed=0).alias("x1"),
            integers(n, -100, high=100, seed=1).alias("x2"),
        ).with_columns(
            ((col("x1") + 2 * col("x2") + normal(n, scale=10.0, seed=2)) / 3).alias("y")
        )

    def _assert_series(self, series: Series, /) -> None:
        df = series.struct.unnest()
        tail = df[-10:]
        # fmt: off
        data = [
            ({"x1": 0.333396198442681, "x2": 0.6845517746145712, "const": 0.2808021232120448}, 59.921571424913495, 1.67032007883995, 0.9955364659986504),
            ({"x1": 0.322785525889542, "x2": 0.6896341527044252, "const": 0.5401793852579858}, 15.974446064929626, -0.3429678871268038, 0.9961762567103958),
            ({"x1": 0.31042868991153927, "x2": 0.7055685710743383, "const": 1.145326562525439}, -31.310827706894123, -0.45191863996575066, 0.998022262986332),
            ({"x1": 0.33311466967931097, "x2": 0.684137842579758, "const": -0.7961518480794516}, 50.66821598287034, -2.975371834066671, 0.9974533939791341),
            ({"x1": 0.35299385150914864, "x2": 0.6758890569593843, "const": -0.9377907849336107}, -0.8749325340834626, 1.0581261048863142, 0.9973453833170313),
            ({"x1": 0.351300641938209, "x2": 0.6456834722890913, "const": -1.859577387752822}, 1.6809655259738476, 0.3217076349681922, 0.9951571413022856),
            ({"x1": 0.3583378199895871, "x2": 0.6588347796692774, "const": -1.109675446287481}, 26.65448170418155, 2.496480675700724, 0.9933751737130443),
            ({"x1": 0.36195632084801765, "x2": 0.658322951215466, "const": -1.3860237983291754}, 7.43453332939416, -0.791818995629618, 0.9905085882663488),
            ({"x1": 0.35564162435283225, "x2": 0.6656931556738634, "const": -0.562680573000551}, -51.90315462605006, 0.6592474497562932, 0.9973576833556038),
            ({"x1": 0.3100421300754357, "x2": 0.675357816881863, "const": 0.48493124625501927}, -36.70039604095908, -0.6071841038068868, 0.9978541580643828),
        ]
        # fmt: on
        expected = DataFrame(
            data=data,
            schema={
                "coefficients": Struct({
                    "x1": Float64,
                    "x2": Float64,
                    "const": Float64,
                }),
                "predictions": Float64,
                "residuals": Float64,
                "R2": Float64,
            },
            orient="row",
        )
        assert_frame_equal(tail, expected)
