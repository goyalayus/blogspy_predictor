# src/utils.py

import logging
import sys
import json
from src.config import LOGS_DIR


class JsonFormatter(logging.Formatter):
    """
    A robust JSON formatter that correctly handles the 'extra' dictionary.
    """

    def format(self, record):
        # Start with the basic attributes from the log record
        log_object = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger_name": record.name,
        }

        # The 'extra' dictionary passed to the logger is stored in the record's __dict__.
        # We need to extract our custom fields without including the standard ones.
        standard_keys = {
            'args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename',
            'funcName', 'levelname', 'levelno', 'lineno', 'message', 'module',
            'msecs', 'msg', 'name', 'pathname', 'process', 'processName',
            'relativeCreated', 'stack_info', 'thread', 'threadName'
        }

        # Add any key from the record's dict that is not a standard key.
        # This is where our 'event', 'url_id', 'insert_stats', etc., will be.
        for key, value in record.__dict__.items():
            if key not in standard_keys:
                log_object[key] = value

        return json.dumps(log_object)


def setup_logging(name: str) -> logging.Logger:
    """
    Configures and returns a logger with dual output:
    1. A human-readable operational log (.log).
    2. A machine-readable performance log (.jsonl).
    """
    LOGS_DIR.mkdir(exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        logger.handlers.clear()

    # --- 1. Operational File Handler (Human-readable) ---
    op_log_file = LOGS_DIR / f"{name}_operational.log"
    op_handler = logging.FileHandler(op_log_file, mode='a')
    op_handler.setLevel(logging.INFO)
    op_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    op_handler.setFormatter(op_formatter)
    logger.addHandler(op_handler)

    # --- 2. Performance File Handler (JSON for analysis) ---
    perf_log_file = LOGS_DIR / f"{name}_performance.jsonl"
    perf_handler = logging.FileHandler(perf_log_file, mode='a')
    perf_handler.setLevel(logging.DEBUG)
    perf_formatter = JsonFormatter()
    perf_handler.setFormatter(perf_formatter)
    logger.addHandler(perf_handler)

    # --- 3. Console Handler (for immediate feedback) ---
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(op_formatter)
    logger.addHandler(console_handler)

    return logger
