from __future__ import annotations

from pathlib import Path

from utilities.core import move

src = Path("/tmp/foo")
src.unlink(missing_ok=True)
src.write_text("source text")
dest = Path("/tmp/bar")
dest = Path("/usr/local/bin/test-bar")
dest.unlink(missing_ok=True)

move(src, dest)
