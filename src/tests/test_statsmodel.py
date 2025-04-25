from __future__ import annotations

from numpy import allclose, isclose, linspace, pi

from utilities.statsmodels import ac_halflife, acf


class TestACF:
    def test_main(self) -> None:
        x = linspace(0, 2 * pi, 1000)
        acfs = acf(x)
        assert acfs.shape == (31,)
        assert isclose(acfs[-1], 0.9100539400539398)

    def test_alpha(self) -> None:
        x = linspace(0, 2 * pi, 1000)
        result = acf(x, alpha=0.5)
        acfs, confint = result
        assert acfs.shape == (31,)
        assert confint.shape == (31, 2)
        assert allclose(confint[-1, :], [0.7534104755915192, 1.0666974045163604])

    def test_qstat(self) -> None:
        x = linspace(0, 2 * pi, 1000)
        result = acf(x, qstat=True)
        acfs, qstat, pvalues = result
        assert acfs.shape == (31,)
        assert qstat.shape == (30,)
        assert isclose(qstat[-1], 27769.955439016478)
        assert pvalues.shape == (30,)
        assert isclose(pvalues[-1], 0.0)

    def test_alpha_and_qstat(self) -> None:
        x = linspace(0, 2 * pi, 1000)
        result = acf(x, alpha=0.5, qstat=True)
        acfs, confint, qstat, pvalues = result
        assert acfs.shape == (31,)
        assert isclose(acfs[-1], 0.9100539400539398)
        assert confint.shape == (31, 2)
        assert allclose(confint[-1, :], [0.7534104755915192, 1.0666974045163604])
        assert qstat.shape == (30,)
        assert isclose(qstat[-1], 27769.955439016478)
        assert pvalues.shape == (30,)
        assert isclose(pvalues[-1], 0.0)


class TestACHalfLife:
    def test_main(self) -> None:
        x = linspace(0, 2 * pi, 1000)
        halflife = ac_halflife(x)
        assert halflife == 169.94
