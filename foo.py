from __future__ import annotations

from utilities.subprocess import run, ssh_keyscan_cmd

run(*ssh_keyscan_cmd("github.com"), print=True)
