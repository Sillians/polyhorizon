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

from polyhorizon.training.configs.settings import load_config

# Service identification
SERVICE_NAME = "polyhorizon.model_training"

# Global state
_setup_done = False
_loggers: Dict[str, logging.Logger] = {}


class ColoredFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
        'RESET': '\033[0m'
    }
    
    def format(self, record: logging.LogRecord) -> str:
        if record.levelname in self.COLORS:
            record.levelname = f"{self.COLORS[record.levelname]}{record.levelname:8s}{self.COLORS['RESET']}"
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


def _get_log_dir(log_dir_value: str) -> Path:
    log_dir = (
        Path(log_dir_value)
        if os.path.isabs(log_dir_value)
        else Path(__file__).parent.parent.parent / log_dir_value
    )
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def _get_log_level(level: str) -> int:
    return getattr(logging, level.upper(), logging.INFO)


def _setup_root_logger() -> None:
    global _setup_done
    if _setup_done:
        return
    
    config = load_config()
    log_config = config.logging
    log_level = _get_log_level(log_config.log_level)
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(
        JSONFormatter()
        if log_config.log_json_format
        else ColoredFormatter(
            fmt=log_config.log_format,
            datefmt=log_config.log_date_format,
        )
    )
    root_logger.addHandler(console_handler)
    
    # File handlers
    if log_config.log_to_file:
        log_dir = _get_log_dir(log_config.log_dir)
        file_formatter = (
            JSONFormatter()
            if log_config.log_json_format
            else logging.Formatter(
                fmt=log_config.log_format,
                datefmt=log_config.log_date_format,
            )
        )
        
        # Main log
        main_handler = logging.handlers.RotatingFileHandler(
            log_dir / "model_training.log",
            maxBytes=log_config.max_file_size,
            backupCount=log_config.backup_count,
            encoding="utf-8",
        )
        main_handler.setLevel(log_level)
        main_handler.setFormatter(file_formatter)
        root_logger.addHandler(main_handler)
        
        # Errors only
        error_handler = logging.handlers.RotatingFileHandler(
            log_dir / "errors.log",
            maxBytes=log_config.max_file_size,
            backupCount=log_config.backup_count,
            encoding="utf-8",
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(file_formatter)
        root_logger.addHandler(error_handler)
    
    # Quiet noisy third-party loggers common in training pipelines
    for noisy in ["urllib3", "requests", "boto3", "botocore", "feast", "fsspec", "s3fs", "torch", "pytorch_lightning", "lightning"]:
        logging.getLogger(noisy).setLevel(logging.WARNING)
    
    _setup_done = True
    logging.getLogger(f"{SERVICE_NAME}.init").info(f"Logging initialized for {SERVICE_NAME}")


def get_logger(name: Optional[str] = None) -> logging.Logger:
    _setup_root_logger()
    full_name = f"{SERVICE_NAME}.{name}" if name and not name.startswith(SERVICE_NAME) else name or SERVICE_NAME
    if full_name not in _loggers:
        _loggers[full_name] = logging.getLogger(full_name)
    return _loggers[full_name]


def log_function_call(func):
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        func_name = func.__name__
        
        # Log function entry
        logger.debug(f"Entering {func_name} with args={args}, kwargs={kwargs}")
        
        try:
            result = func(*args, **kwargs)
            logger.debug(f"Exiting {func_name} with result: {result}")
            return result
        except Exception as e:
            logger.error(f"Exception in {func_name}: {e}", exc_info=True)
            raise
    
    return wrapper


def log_execution_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        start = time.time()
        try:
            result = func(*args, **kwargs)
            logger.info(f"{func.__qualname__} executed in {time.time() - start:.4f}s")
            return result
        except Exception:
            logger.error(f"{func.__qualname__} failed after {time.time() - start:.4f}s", exc_info=True)
            raise
    return wrapper
