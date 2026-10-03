"""Logging must be usable in a clean checkout without service credentials."""

import os
from pathlib import Path
import subprocess
import sys


def test_logging_without_dotenv_or_service_configuration(tmp_path):
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-c", """
import logging
from polyhorizon.core.logger import get_logger, get_log_level, should_log_to_file
assert get_log_level() == logging.INFO
assert not should_log_to_file()
get_logger('clean-checkout').info('Core logging is available')

from polyhorizon.training.configs import settings
def forbid_config_load(*args, **kwargs):
    raise AssertionError('Acquiring a logger must not load training config')
settings.load_config = forbid_config_load
from polyhorizon.training.utils.logger import get_logger as training_logger
assert training_logger('test').name == 'polyhorizon.model_training.test'
"""],
        cwd=tmp_path,
        env={"PATH": os.environ.get("PATH", ""), "PYTHONPATH": str(root),
             "PYTHON_DOTENV_DISABLED": "1", "LOG_FORMAT": "json"},
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert not list(tmp_path.iterdir())


def test_training_logging_uses_explicit_config_and_preserves_root(tmp_path):
    import logging
    from polyhorizon.training.configs.settings import LoggingConfig
    from polyhorizon.training.utils.logger import SERVICE_NAME, configure_logging, get_logger

    service = logging.getLogger(SERVICE_NAME)
    old_handlers, old_level, old_propagate = service.handlers[:], service.level, service.propagate
    service.handlers.clear()
    root_handlers = logging.getLogger().handlers[:]
    config = LoggingConfig(
        log_level="INFO", log_dir=str(tmp_path), log_format="%(message)s",
        log_date_format="%Y-%m-%d", log_to_file=True, log_json_format=False,
        max_file_size=1024, backup_count=1, alert_emails=[],
    )
    try:
        logger = get_logger("explicit-config")
        configure_logging(config)
        configure_logging(config)
        logger.info("configured once")
        assert (tmp_path / "model_training.log").read_text().count("configured once") == 1
        assert logging.getLogger().handlers == root_handlers
    finally:
        for handler in service.handlers[:]:
            service.removeHandler(handler)
            handler.close()
        service.handlers[:] = old_handlers
        service.setLevel(old_level)
        service.propagate = old_propagate
