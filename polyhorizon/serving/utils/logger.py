from __future__ import annotations

import functools
import json
import logging
import logging.handlers
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from polyhorizon.serving.configs.settings import load_config

# Global state
_setup_done = False
_service_name = "polyhorizon.model_serving"
_loggers: Dict[str, logging.Logger] = {}


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
    def __init__(self, service_name: str):
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "service": self.service_name,
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


def _get_log_dir(log_dir: str) -> Path:
    base = Path(log_dir) if Path(log_dir).is_absolute() else Path(__file__).parent.parent.parent / log_dir
    base.mkdir(parents=True, exist_ok=True)
    return base


def _get_log_level(level: str) -> int:
    return getattr(logging, level.upper(), logging.INFO)


def configure_logging_from_settings() -> None:
    global _setup_done, _service_name
    if _setup_done:
        return

    try:
        config = load_config(os.getenv("SERVING_CONFIG_PATH"))
    except (FileNotFoundError, ValueError):
        # Importing a service module must not require the entire runtime
        # environment. This fallback is used by tests, CLIs, and migrations;
        # container startup still validates the full serving configuration.
        logging.basicConfig(level=logging.INFO, stream=sys.stdout)
        _setup_done = True
        return

    log_cfg = config.logging
    service_name = config.project.service_name
    _service_name = service_name

    log_level = _get_log_level(log_cfg.log_level)
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    if log_cfg.log_json_format:
        console_handler.setFormatter(JSONFormatter(service_name))
    else:
        console_handler.setFormatter(
            ColoredFormatter(fmt=log_cfg.log_format, datefmt=log_cfg.log_date_format)
        )
    root_logger.addHandler(console_handler)

    if log_cfg.log_to_file:
        log_dir = _get_log_dir(log_cfg.log_dir)
        file_formatter = (
            JSONFormatter(service_name)
            if log_cfg.log_json_format
            else logging.Formatter(fmt=log_cfg.log_format, datefmt=log_cfg.log_date_format)
        )

        main_handler = logging.handlers.RotatingFileHandler(
            log_dir / "model_serving.log",
            maxBytes=log_cfg.max_file_size,
            backupCount=log_cfg.backup_count,
            encoding="utf-8",
        )
        main_handler.setLevel(log_level)
        main_handler.setFormatter(file_formatter)
        root_logger.addHandler(main_handler)

        error_handler = logging.handlers.RotatingFileHandler(
            log_dir / "errors.log",
            maxBytes=log_cfg.max_file_size,
            backupCount=log_cfg.backup_count,
            encoding="utf-8",
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(file_formatter)
        root_logger.addHandler(error_handler)

    for noisy in ["urllib3", "requests", "boto3", "botocore", "uvicorn", "fastapi", "redis", "feast"]:
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _setup_done = True


def get_logger(name: Optional[str] = None) -> logging.Logger:
    configure_logging_from_settings()

    if name is None:
        return logging.getLogger()

    full_name = f"{_service_name}.{name}" if not name.startswith(_service_name) else name
    if full_name not in _loggers:
        _loggers[full_name] = logging.getLogger(full_name)
    return _loggers[full_name]


def log_execution_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            logger.info("%s executed in %.4fs", func.__qualname__, duration)
            return result
        except Exception:
            duration = time.time() - start_time
            logger.error("%s failed after %.4fs", func.__qualname__, duration, exc_info=True)
            raise
    return wrapper


class LogContext:
    def __init__(self, **context):
        self.context = context
        self.old_factory = None

    def __enter__(self):
        old_factory = logging.getLogRecordFactory()

        def record_factory(*args, **kwargs):
            record = old_factory(*args, **kwargs)
            record.extra_fields = self.context
            return record

        logging.setLogRecordFactory(record_factory)
        self.old_factory = old_factory
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.old_factory:
            logging.setLogRecordFactory(self.old_factory)
