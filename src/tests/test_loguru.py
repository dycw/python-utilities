from os import chdir
from pathlib import Path
from re import search

from loguru import logger

from dycw_utilities.loguru import setup_loguru


class TestSetupLoguru:
    def test_main(self, tmp_path: Path) -> None:
        chdir(tmp_path)
        setup_loguru()

        logger.debug("message")

        (log,) = tmp_path.iterdir()
        assert log.name == "log"
        with open(log) as fh:
            (line,) = fh.read().splitlines()

        assert search(
            r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| DEBUG\s* \| "
            + r"tests\.test_loguru:test_main:\d+ - message$",
            line,
        )
