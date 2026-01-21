from __future__ import annotations

from pathlib import Path
from shutil import rmtree

from utilities.core import move

src = Path("/tmp/foo")
rmtree(src, ignore_errors=True)
src.write_text("source text")
dest = Path("/usr/local/bin/test-bar")
dest.mkdir()
(dest / "orig1.txt").touch()
(dest / "orig2.txt").touch()
(dest / "orig3.txt").touch()
# move(src, dest)
