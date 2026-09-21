from __future__ import annotations

import functools
import json
import logging
import logging.handlers
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional


# Default values (override via settings)
LOG_LEVEL = "INFO"
LOG_DIR = "logs"
LOG_FORMAT = "%(asctime)s | %(name)s | %(levelname)s | %(funcName)s:%(lineno)d | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_TO_FILE = True
LOG_JSON_FORMAT = False
MAX_FILE_SIZE = 10 * 1024 * 1024
BACKUP_COUNT = 5

SERVICE_NAME = "polyhorizon.feature_store"

_setup_done = False
_loggers: Dict[str, logging.Logger] = {}
_configured_from_settings = False


class ColoredFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": "\033[36m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[35m",
        "RESET": "\033[0m",
    }

    def format(self, record: logging.LogRecord) -> str:
        if record.levelname in self.COLORS:
            record.levelname = (
                f"{self.COLORS[record.levelname]}{record.levelname:8s}{self.COLORS['RESET']}"
            )
        return super().format(record)


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "service": SERVICE_NAME,
            "level": record.levelname,
            "logger": record.name,
            "function": record.funcName,
            "line": record.lineno,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        if hasattr(record, "extra_fields"):
            log_data["context"] = record.extra_fields
        return json.dumps(log_data)


def _get_log_dir() -> Path:
    log_dir = Path(LOG_DIR)
    if not log_dir.is_absolute():
        log_dir = Path(__file__).parent.parent.parent / LOG_DIR
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def _get_log_level() -> int:
    return getattr(logging, LOG_LEVEL, logging.INFO)


def _setup_root_logger() -> None:
    global _setup_done
    if _setup_done:
        return

    log_level = _get_log_level()
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(
        JSONFormatter() if LOG_JSON_FORMAT else ColoredFormatter(fmt=LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
    )
    root_logger.addHandler(console_handler)

    if LOG_TO_FILE:
        log_dir = _get_log_dir()
        file_formatter = JSONFormatter() if LOG_JSON_FORMAT else logging.Formatter(
            fmt=LOG_FORMAT, datefmt=LOG_DATE_FORMAT
        )

        main_handler = logging.handlers.RotatingFileHandler(
            log_dir / "feature_store.log",
            maxBytes=MAX_FILE_SIZE,
            backupCount=BACKUP_COUNT,
            encoding="utf-8",
        )
        main_handler.setLevel(log_level)
        main_handler.setFormatter(file_formatter)
        root_logger.addHandler(main_handler)

        error_handler = logging.handlers.RotatingFileHandler(
            log_dir / "errors.log",
            maxBytes=MAX_FILE_SIZE,
            backupCount=BACKUP_COUNT,
            encoding="utf-8",
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(file_formatter)
        root_logger.addHandler(error_handler)

    for noisy in ["urllib3", "requests", "boto3", "botocore", "feast", "google", "grpc", "redis"]:
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _setup_done = True
    logging.getLogger(f"{SERVICE_NAME}.init").info("Logging initialized for %s", SERVICE_NAME)


def configure_logging(logging_config) -> None:
    global LOG_LEVEL, LOG_DIR, LOG_FORMAT, LOG_DATE_FORMAT, LOG_TO_FILE, LOG_JSON_FORMAT
    global MAX_FILE_SIZE, BACKUP_COUNT, _setup_done

    LOG_LEVEL = str(getattr(logging_config, "log_level", LOG_LEVEL)).upper()
    LOG_DIR = getattr(logging_config, "log_dir", LOG_DIR)
    LOG_FORMAT = getattr(logging_config, "log_format", LOG_FORMAT)
    LOG_DATE_FORMAT = getattr(logging_config, "log_date_format", LOG_DATE_FORMAT)
    LOG_TO_FILE = bool(getattr(logging_config, "log_to_file", LOG_TO_FILE))
    LOG_JSON_FORMAT = bool(getattr(logging_config, "log_json_format", LOG_JSON_FORMAT))
    MAX_FILE_SIZE = int(getattr(logging_config, "max_file_size", MAX_FILE_SIZE))
    BACKUP_COUNT = int(getattr(logging_config, "backup_count", BACKUP_COUNT))

    _setup_done = False
    _setup_root_logger()


def configure_logging_from_settings() -> None:
    global _configured_from_settings
    if _configured_from_settings:
        return
    try:
        from polyhorizon.features.configs.settings import load_config

        config = load_config()
        configure_logging(config.logging)
        _configured_from_settings = True
    except Exception as exc:
        if not _setup_done:
            _setup_root_logger()
        logging.getLogger(f"{SERVICE_NAME}.init").warning(
            "Failed to load feature logging settings; using defaults: %s",
            exc,
        )


def get_logger(name: Optional[str] = None) -> logging.Logger:
    if not _setup_done:
        configure_logging_from_settings()
    full_name = f"{SERVICE_NAME}.{name}" if name and not name.startswith(SERVICE_NAME) else name or SERVICE_NAME
    if full_name not in _loggers:
        _loggers[full_name] = logging.getLogger(full_name)
    return _loggers[full_name]


def log_function_call(func):
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        func_name = func.__name__
        logger.debug("Entering %s with args=%s, kwargs=%s", func_name, args, kwargs)
        try:
            result = func(*args, **kwargs)
            logger.debug("Exiting %s with result=%s", func_name, result)
            return result
        except Exception:
            logger.error("Exception in %s", func_name, exc_info=True)
            raise

    return wrapper


def log_execution_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        start = time.time()
        try:
            result = func(*args, **kwargs)
            logger.info("%s executed in %.4fs", func.__qualname__, time.time() - start)
            return result
        except Exception:
            logger.error("%s failed after %.4fs", func.__qualname__, time.time() - start, exc_info=True)
            raise

    return wrapper


configure_logging_from_settings()
