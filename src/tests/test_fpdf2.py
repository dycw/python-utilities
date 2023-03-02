from holoviews import Curve
from hypothesis import given
from pytest import mark

from utilities.fpdf2 import yield_pdf
from utilities.hypothesis import text_ascii


class TestYieldPDF:
    @given(text=text_ascii(min_size=1))
    def test_add_fixed_width_text(self, text: str) -> None:
        with yield_pdf() as pdf:
            pdf.add_fixed_width_text(text)

    @mark.xfail(reason="firefox/geckodriver")
    def test_add_plot(self) -> None:
        curve = Curve([])
        with yield_pdf() as pdf:
            pdf.add_plot(curve)

    @given(header=text_ascii(min_size=1))
    def test_header(self, header: str) -> None:
        with yield_pdf(header=header) as pdf:
            pdf.header()

    def test_footer(self) -> None:
        with yield_pdf() as pdf:
            pdf.footer()
