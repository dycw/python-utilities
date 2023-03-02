import datetime as dt
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from typing import Optional

from beartype import beartype
from fpdf import FPDF
from fpdf.enums import XPos
from fpdf.enums import YPos

from utilities.datetime import local_timezone
from utilities.holoviews import save_plot
from utilities.tempfile import TemporaryDirectory


class _BasePDF(FPDF):
    """Base class for PDFs."""

    @beartype
    def add_fixed_width_text(self, text: str, /) -> None:
        """Add a block of fixed witth text."""
        self.set_font("Courier")
        self.write(txt=text)
        self.ln()

    @beartype
    def add_plot(self, plot: Any, /) -> None:  # pragma: no cover
        with TemporaryDirectory() as temp:
            path = temp.joinpath("image.png")
            save_plot(plot, "plot.png")
            _ = self.image(path, w=self.epw)


@contextmanager
@beartype
def yield_pdf(*, header: Optional[str] = None) -> Iterator[_BasePDF]:
    """Yield a PDF."""

    class OutputPDF(_BasePDF):
        @beartype
        def header(self) -> None:
            if header is not None:
                self.set_font(family="Helvetica", style="B", size=15)
                _ = self.cell(w=80)
                _ = self.cell(
                    w=30,
                    h=10,
                    txt=header,
                    border=0,
                    align="C",
                    new_x=XPos.RIGHT,
                    new_y=YPos.TOP,
                )

        @beartype
        def footer(self) -> None:
            self.set_y(-15)
            self.set_font(family="Helvetica", style="I", size=8)
            page_no, now = self.page_no, dt.datetime.now(tz=local_timezone())
            txt = f"page {page_no}/{{nb}}; {now:%Y-%m-%d %H:%M:%S}"
            _ = self.cell(
                w=0,
                h=10,
                txt=txt,
                border=0,
                align="C",
                new_x=XPos.RIGHT,
                new_y=YPos.TOP,
            )

    pdf = OutputPDF(orientation="portrait", unit="mm", format="A4")
    pdf.alias_nb_pages()
    pdf.add_page()
    pdf.set_font("Helvetica", size=10)
    yield pdf
