from threading import get_native_id
from time import sleep
from typing import Any
from typing import cast

from click import command
from click import option
from loguru import logger
from luigi import IntParameter
from luigi import Task
from numpy import arange
from numpy import array
from numpy.random import default_rng
from numpy.random import random

from utilities.atomicwrites import writer
from utilities.class_name import get_class_name
from utilities.logging import LogLevel
from utilities.loguru import setup_loguru
from utilities.luigi import PathTarget
from utilities.luigi import build
from utilities.os import CPU_COUNT
from utilities.tempfile import TEMP_DIR


class Example(Task):
    """Example task."""

    messages = cast(int, IntParameter())

    def output(self) -> PathTarget:
        return PathTarget(TEMP_DIR.joinpath(get_class_name(self)))

    def run(self) -> None:
        rng = default_rng(get_native_id())
        levels = [level.name for level in LogLevel]
        p = array([40, 30, 20, 9, 1]) / 100.0
        for i in arange(n := self.messages) + 1:
            level = rng.choice(levels, p=p)
            logger.log(level, "{}, #{}/{}", get_class_name(self), i, n)
            sleep(1.0 + random())
        with writer(self.output().path) as temp:
            temp.touch()


@command()
@option("-t", "--tasks", default=10)
@option("-m", "--messages", default=60)
def main(*, tasks: int, messages: int) -> None:
    """Run the test script."""
    setup_loguru(levels={"luigi": LogLevel.DEBUG}, files="test_luigi")
    classes = [type(f"Example{i}", (Example,), {}) for i in range(tasks)]
    instances = [cast(Example, cast(Any, cls)(messages=messages)) for cls in classes]
    _ = build(instances, local_scheduler=True, workers=CPU_COUNT)


if __name__ == "__main__":
    main()
