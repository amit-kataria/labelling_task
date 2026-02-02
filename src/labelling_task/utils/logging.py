from __future__ import annotations

import logging
import sys


def configure_logging(level: str = "INFO") -> None:
    """
    Structured-enough logging for ops users.

    In production you would route this to files and/or a log aggregator.
    """
    root = logging.getLogger()
    root.setLevel(level.upper())

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    handler.setFormatter(formatter)

    # Replace existing handlers to avoid duplicates under reload.
    root.handlers = [handler]
