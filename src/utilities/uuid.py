from __future__ import annotations

import re

UUID_PATTERN = re.compile(
    "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
)


__all__ = ["UUID_PATTERN"]
