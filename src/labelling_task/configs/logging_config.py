import logging
import logging.config
import sys
from datetime import datetime


class LogFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.utcfromtimestamp(record.created)
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def _rclip(self, val, max_len):
        val = str(val)
        return val if len(val) <= max_len else val[: max_len - 3]

    def _lclip(self, val, max_len):
        val = str(val)
        if len(val) <= max_len:
            return val
        return val[-(max_len - 3) :]

    def format(self, record):
        # Apply clipping to all fields
        record.threadName = self._lclip(record.threadName, 10)
        record.levelname = self._lclip(record.levelname, 5)
        record.filename = self._lclip(record.filename, 25)
        return super().format(record)


FORMAT = "%(asctime)s %(levelname)-5s %(threadName)-10s %(filename)25s:%(lineno)4d - %(message)s"


def setup_logging():
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"log4j": {"()": LogFormatter, "format": FORMAT}},
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "log4j",
                "stream": "ext://sys.stdout",
            }
        },
        "root": {"level": "DEBUG", "handlers": ["console"]},
        "loggers": {
            "uvicorn": {"level": "INFO", "handlers": ["console"], "propagate": False},
            "uvicorn.access": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False,
            },
            "uvicorn.error": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False,
            },
            "urllib3": {"level": "WARNING"},
            "requests": {"level": "WARNING"},
            "motor": {"level": "WARNING"},
            "pymongo": {"level": "WARNING"},
            "pymongo.command": {"level": "WARNING"},
            "pymongo.connection": {"level": "WARNING"},
            "pymongo.serverSelection": {"level": "WARNING"},
            "redis": {"level": "WARNING"},
        },
    }

    logging.config.dictConfig(logging_config)


def get_logger(name: str):
    return logging.getLogger(name)
