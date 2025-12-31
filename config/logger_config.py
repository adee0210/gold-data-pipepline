import logging
import os
from logging.handlers import RotatingFileHandler


class LoggerConfig:

    @staticmethod
    def logger_config(
        log_name: str,
        log_file: str = None,
        log_level: int = None,
        max_bytes: int = None,
        backup_count: int = None,
    ):
        # Import here to avoid circular dependency
        from config.variable_config import LOGGING_CONFIG

        # Sử dụng giá trị từ config nếu không được truyền vào
        log_file = log_file or LOGGING_CONFIG["log_file"]
        log_level = log_level or getattr(
            logging, LOGGING_CONFIG["log_level"], logging.INFO
        )
        max_bytes = max_bytes or LOGGING_CONFIG["max_bytes"]
        backup_count = backup_count or LOGGING_CONFIG["backup_count"]

        # Lấy thư mục gốc của project (đảm bảo log luôn nằm trong project)
        # __file__ -> config/logger_config.py
        # dirname(__file__) -> /path/to/project/config
        # dirname(dirname(__file__)) -> /path/to/project
        config_dir = os.path.dirname(os.path.abspath(__file__))
        root_dir = os.path.dirname(config_dir)
        base_path = os.path.join(root_dir, log_file)

        # formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(processName)s - %(levelname)s - %(name)s - %(message)s"
        )

        # RotatingFileHandler: Tự động rotate khi file đạt maxBytes
        file_handler = RotatingFileHandler(
            filename=base_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)

        # Ensure console logging is disabled
        # console_handler = logging.StreamHandler()
        # console_handler.setFormatter(formatter)

        logger = logging.getLogger(log_name)

        if not logger.handlers:
            list_handler = [file_handler]  # Only file handler remains
            for h in list_handler:
                logger.addHandler(h)

        logger.propagate = False
        logger.setLevel(log_level)
        return logger
