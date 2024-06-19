from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from fpdf import FPDF
from fpdf.enums import XPos, YPos
from typing_extensions import override

from utilities.datetime import get_now, local_timezone
from utilities.pathlib import ensure_path
from utilities.tempfile import TemporaryDirectory

if TYPE_CHECKING:
    from collections.abc import Iterator


class _BasePDF(FPDF):
    """Base class for PDFs."""

    def add_fixed_width_text(self, text: str, /) -> None:
        """Add a block of fixed witth text."""
        self.set_font("Courier")
        _ = self.write(text=text)
        self.ln()

    def add_plot(
        self, plot: Any, /, *, validate: bool = False
    ) -> None:  # pragma: no cover
        from utilities.holoviews import save_plot

        with TemporaryDirectory() as temp:
            path = ensure_path(temp, "image.png", validate=validate)
            save_plot(plot, path)
            _ = self.image(path, w=self.epw)


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
            page_no, now = self.page_no(), get_now(tz=local_timezone())
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
