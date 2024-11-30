from __future__ import annotations

import sys
from io import StringIO

buffer = StringIO()


def custom_excepthook(exc_type, exc_value, exc_traceback) -> None:
    print("""HELLO THERE""", file=buffer)
    print("Custom exception hook called", file=buffer)
    print(f"Exception type: {exc_type}", file=buffer)
    print(f"Exception value: {exc_value}", file=buffer)
    print(buffer.getvalue())


sys.excepthook = custom_excepthook

msg = "Test exception"
raise ValueError(msg)
