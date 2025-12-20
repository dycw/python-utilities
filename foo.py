from __future__ import annotations

from utilities.subprocess import run

run(
    """\
for i in $(seq 1 10); do
    if [ $((i % 2)) -eq 1 ]; then
        echo "stdout $i"
    else
        echo "stderr $i" 1>&2
    fi
    sleep 1
done
""",
    shell=True,
    print=True,
)
# run("./loopWithSleep.sh", shell=True, print=True)
