from __future__ import annotations

from socket import gethostname

HOSTNAME = gethostname()


__all__ = ["HOSTNAME"]
