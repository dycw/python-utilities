from os import chdir
from pathlib import Path
from re import search
from time import sleep

from loguru import logger

from utilities.loguru import setup_loguru


class TestSetupLoguru:
    def test_main(self, tmp_path: Path) -> None:
        chdir(tmp_path)
        setup_loguru()

        sleep(1e-3)
        logger.debug("message")
        sleep(1e-3)

        (log,) = tmp_path.iterdir()
        assert log.name == "log"
        with open(log) as fh:
            (line,) = fh.read().splitlines()

        assert search(
            r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| DEBUG\s* \| "
            + r"tests\.test_loguru:test_main:\d+ - message$",
            line,
        )
