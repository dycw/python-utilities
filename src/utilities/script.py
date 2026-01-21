from __future__ import annotations

from pathlib import Path
from shutil import rmtree

from utilities.core import move

src = Path("/tmp/foo")
src.unlink(missing_ok=True)
src.write_text("source text")
dest = Path("/usr/local/bin/test-bar")
rmtree(dest, ignore_errors=True)
dest.mkdir()
(dest / "orig1.txt").touch()
(dest / "orig2.txt").touch()
(dest / "orig3.txt").touch()
# move(src, dest)
