from __future__ import annotations

UUID_PATTERN = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
UUID_EXACT_PATTERN = f"^{UUID_PATTERN}$"


__all__ = ["UUID_EXACT_PATTERN", "UUID_PATTERN"]
