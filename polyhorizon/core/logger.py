import os
import sys
import time
import logging
import logging.handlers
from pathlib import Path
from typing import Optional
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()


DEFAULT_LOG_LEVEL = os.environ.get("DEFAULT_LOG_LEVEL")
DEFAULT_LOG_DIR = os.environ.get("DEFAULT_LOG_DIR")
DEFAULT_LOG_FORMAT = os.environ.get("DEFAULT_LOG_FORMAT")
DEFAULT_DATE_FORMAT = os.environ.get("DEFAULT_DATE_FORMAT")
LOG_TO_FILE = os.environ.get("LOG_TO_FILE")
MAX_FILE_SIZE = int(os.environ.get("MAX_FILE_SIZE", 10485760)) 
BACKUP_COUNT = int(os.environ.get("BACKUP_COUNT", 5)) 

_setup_done = False
_loggers = {}


class ColoredFormatter(logging.Formatter):
    """Custom formatter that adds colors to console output."""
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
        'RESET': '\033[0m'      # Reset
    }
    
    def format(self, record):
        if record.levelname in self.COLORS:
            record.levelname = (
                f"{self.COLORS[record.levelname]}{record.levelname}{self.COLORS['RESET']}"
            )
        return super().format(record)


def get_log_level() -> int:
    """Get log level from environment variable or default."""
    level_name = os.getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()
    return getattr(logging, level_name, logging.INFO)


def get_log_dir() -> Path:
    """Get log directory from environment variable or default."""
    log_dir = os.getenv("LOG_DIR", DEFAULT_LOG_DIR)
    if not os.path.isabs(log_dir):
        project_root = Path(__file__).parent.parent
        log_dir = project_root / log_dir
    else:
        log_dir = Path(log_dir)
    
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def should_log_to_file() -> bool:
    """Check if file logging is enabled."""
    return os.getenv("LOG_TO_FILE", "true").lower() in ("true", "1", "yes", "on")


def setup_logging() -> None:
    """Setup the root logger with console and file handlers."""
    global _setup_done
    
    if _setup_done:
        return
    
    log_level = get_log_level()
    
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    root_logger.handlers.clear()
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = ColoredFormatter(
        fmt=DEFAULT_LOG_FORMAT,
        datefmt=DEFAULT_DATE_FORMAT
    )
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    

    if should_log_to_file():
        log_dir = get_log_dir()
        
        main_log_file = log_dir / "polyhorizon.log"
        file_handler = logging.handlers.RotatingFileHandler(
            main_log_file,
            maxBytes=MAX_FILE_SIZE,
            backupCount=BACKUP_COUNT,
            encoding='utf-8'
        )
        file_handler.setLevel(log_level)
        
        file_formatter = logging.Formatter(
            fmt=DEFAULT_LOG_FORMAT,
            datefmt=DEFAULT_DATE_FORMAT
        )
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)
        
        error_log_file = log_dir / "errors.log"
        error_handler = logging.handlers.RotatingFileHandler(
            error_log_file,
            maxBytes=MAX_FILE_SIZE,
            backupCount=BACKUP_COUNT,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(file_formatter)
        root_logger.addHandler(error_handler)
    
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("selenium").setLevel(logging.WARNING)
    
    _setup_done = True
    
    logger = logging.getLogger("core.logger")
    logger.info(f"Logging initialized - Level: {logging.getLevelName(log_level)}")
    if should_log_to_file():
        logger.info(f"Log files location: {get_log_dir()}")


def get_logger(name: Optional[str] = None) -> logging.Logger:
    setup_logging()
    
    if name is None:
        return logging.getLogger()
    
    if name not in _loggers:
        _loggers[name] = logging.getLogger(name)
    
    return _loggers[name]


def log_function_call(func):
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        func_name = func.__name__
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
    
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        func_name = func.__name__
        
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(f"{func_name} executed in {execution_time:.4f} seconds")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"{func_name} failed after {execution_time:.4f} seconds: {e}")
            raise
    
    return wrapper


class LoggerContext:
    def __init__(self, level: str, logger_name: Optional[str] = None):
        self.level = getattr(logging, level.upper())
        self.logger = get_logger(logger_name)
        self.original_level = None
    
    def __enter__(self):
        self.original_level = self.logger.level
        self.logger.setLevel(self.level)
        return self.logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.setLevel(self.original_level)


# Convenience functions for common logging patterns
def log_startup(app_name: str, version: str = "1.0.0") -> None:
    """Log application startup information."""
    logger = get_logger("startup")
    logger.info("=" * 60)
    logger.info(f"Starting {app_name} v{version}")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Log level: {logging.getLevelName(get_log_level())}")
    # logger.info(f"Working directory: {os.getcwd()}")
    logger.info("=" * 60)


def log_shutdown(app_name: str) -> None:
    """Log application shutdown information."""
    logger = get_logger("shutdown")
    logger.info("=" * 60)
    logger.info(f"Shutting down {app_name}")
    logger.info(f"Shutdown completed at {datetime.now().isoformat()}")
    logger.info("=" * 60)


def log_config_info() -> None:
    """Log current logging configuration."""
    logger = get_logger("config")
    logger.info("Current logging configuration:")
    logger.info(f"  Log Level: {logging.getLevelName(get_log_level())}")
    logger.info(f"  File Logging: {should_log_to_file()}")
    if should_log_to_file():
        logger.info(f"  Log Directory: {get_log_dir()}")
    logger.info(f"  Setup Complete: {_setup_done}")


# Initialize logging when module is imported
setup_logging()