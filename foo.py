from __future__ import annotations

from utilities.subprocess import run

# run("for i in $(seq 1 10); do echo hi $i; sleep 1; done", shell=True, print=True)
run(
    'bash -c \'for i in $(seq 1 5); do printf "hi %s\n" "$i"; sleep 1; done\'',
    shell=True,
    print=True,
)
