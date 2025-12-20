from __future__ import annotations

from utilities.subprocess import run

run("for i in $(seq 1 5); do echo hi $i; sleep 1; done", shell=True, print=True)
# run("./loopWithSleep.sh", shell=True, print=True)
