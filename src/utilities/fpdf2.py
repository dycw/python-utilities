from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, override

from fpdf import FPDF
from fpdf.enums import XPos, YPos

from utilities.datetime import get_now_local

if TYPE_CHECKING:
    from collections.abc import Iterator


class _BasePDF(FPDF):
    """Base class for PDFs."""

    def add_fixed_width_text(self, text: str, /) -> None:
        """Add a block of fixed witth text."""
        self.set_font("Courier")
        _ = self.write(text=text)
        self.ln()


@contextmanager
def yield_pdf(*, header: str | None = None) -> Iterator[_BasePDF]:
    """Yield a PDF."""

    class OutputPDF(_BasePDF):
        @override
        def header(self) -> None:
            if header is not None:
                self.set_font(family="Helvetica", style="B", size=15)
                _ = self.cell(w=80)
                _ = self.cell(
                    w=30,
                    h=10,
                    text=header,
                    border=0,
                    align="C",
                    new_x=XPos.RIGHT,
                    new_y=YPos.TOP,
                )
                self.ln(20)

        @override
        def footer(self) -> None:
            self.set_y(-15)
            self.set_font(family="Helvetica", style="I", size=8)
            page_no, now = self.page_no(), get_now_local()
            text = f"page {page_no}/{{}}; {now}"
            _ = self.cell(
                w=0,
                h=10,
                text=text,
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


__all__ = ["yield_pdf"]
