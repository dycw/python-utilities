from __future__ import annotations

from holoviews import Curve
from hypothesis import given
from typing_extensions import Self

from utilities.fpdf2 import yield_pdf
from utilities.hypothesis import text_ascii
from utilities.pytest import skipif_not_linux


class TestYieldPDF:
    @given(text=text_ascii(min_size=1))
    def test_add_fixed_width_text(self: Self, *, text: str) -> None:
        with yield_pdf() as pdf:
            pdf.add_fixed_width_text(text)

    @skipif_not_linux
    def test_add_plot(self: Self) -> None:
        curve = Curve([])
        with yield_pdf() as pdf:
            pdf.add_plot(curve)

    @given(header=text_ascii(min_size=1))
    def test_header(self: Self, *, header: str) -> None:
        with yield_pdf(header=header) as pdf:
            pdf.header()

    def test_footer(self: Self) -> None:
        with yield_pdf() as pdf:
            pdf.footer()
