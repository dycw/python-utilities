from __future__ import annotations

from csv import reader
from itertools import chain
from typing import TYPE_CHECKING

from click import command
from loguru import logger
from mdutils import MdUtils

from utilities.iterables import one
from utilities.loguru import setup_loguru
from utilities.pathlib import ensure_path
from utilities.scripts.csv_to_markdown.classes import Config
from utilities.tempfile import TemporaryDirectory
from utilities.typed_settings import click_options

if TYPE_CHECKING:
    from pathlib import Path

_CONFIG = Config()


@command()
@click_options(Config, appname="csvtomarkdown")
def main(config: Config, /) -> None:
    """CLI for the `csv_to_markdown` script."""
    setup_loguru()
    markdown = _csv_to_markdown(path=config.path)
    logger.info(f"{markdown}")


def _csv_to_markdown(*, path: Path = _CONFIG.path) -> str:
    with path.open() as fh:
        csv_reader = reader(fh)
        rows = list(csv_reader)
    n_rows = len(rows)
    n_columns = one(set(map(len, rows)))
    flattened = list(chain(*rows))
    with TemporaryDirectory() as temp_dir:
        temp_file = ensure_path(temp_dir, "temp.md")
        md_file = MdUtils(file_name=str(temp_file))
        _ = md_file.new_table(n_columns, n_rows, flattened)
        _ = md_file.create_md_file()
        with temp_file.open() as fh:
            return fh.read().strip("\n")
