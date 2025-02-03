import os
import logging
import yaml
from logging import Logger, StreamHandler, Formatter
from logging.handlers import TimedRotatingFileHandler
from typing import Optional


class DriverLogger:
    def __init__(
        self,
        name: str = "default",
        log_file: Optional[str] = "default.log",
        when: str = "midnight",
        interval: int = 1,
        backup_count: int = 7,
        level: int = logging.INFO,
        config_path: str = "configs/app.yaml",
    ):
        """
        Initialize a time-rotating logger.

        :param name: Unique name for the logger
        :param log_file: Path to the log file
        :param when: Rotation interval ('S', 'M', 'H', 'D', 'midnight')
        :param interval: How often to rotate the log file
        :param backup_count: Number of backup files to keep
        :param level: Logging level
        """
        self.config_path = config_path
        self.base_logger_config = self._load_config()
        log_directory = self.base_logger_config.get("log_directory")
        self.ensure_directory_exists(log_directory)
        log_file = os.path.join(log_directory, log_file)
        level = getattr(logging, self.base_logger_config.get("log_level").upper(), None)
        when = self.base_logger_config.get("when")
        interval = self.base_logger_config.get("interval")
        backup_count = self.base_logger_config.get("backup_count")
        timed_rotating_file_handler = self.base_logger_config.get("stream_handler")

        self.logger = Logger(name)
        self.logger.setLevel(level)

        # Ensure no handlers are accidentally attached
        self.logger.handlers.clear()

        # Create and configure handlers
        self._setup_stream_handler()
        self._setup_timed_rotating_file_handler(log_file, when, interval, backup_count)

    def ensure_directory_exists(self, filepath: str):
        """
        Ensures that the directory for the given file path exists.
        If it doesn't exist, the directory is created.

        :param filepath: Path to the file, including the file name.
        """
        directory = os.path.dirname(filepath)
        if directory:  # Ensure the directory part is not empty
            os.makedirs(directory, exist_ok=True)

    def _load_config(self):
        """
        Load configuration from a YAML file.
        :return: Configuration dictionary
        """
        try:
            with open(self.config_path, "r") as file:
                app_logger_config = yaml.safe_load(file)["base_logger"]
            return app_logger_config
        except Exception as e:
            raise Exception(f"Error loading configuration: {e}")

    def _setup_stream_handler(self):
        """Set up a stream handler for console logging."""
        stream_handler = StreamHandler()
        stream_handler_format = self.base_logger_config.get("stream_handler_format")

        stream_handler.setFormatter(Formatter(stream_handler_format))
        self.logger.addHandler(stream_handler)

    def _setup_timed_rotating_file_handler(
        self, log_file: str, when: str, interval: int, backup_count: int
    ):
        """Set up a timed rotating file handler."""
        rotating_handler = TimedRotatingFileHandler(
            log_file, when=when, interval=interval, backupCount=backup_count
        )
        timed_rotating_file_handler_format = self.base_logger_config.get(
            "timed_rotating_file_handler_format"
        )
        rotating_handler.setFormatter(Formatter(timed_rotating_file_handler_format))
        self.logger.addHandler(rotating_handler)

    def get_logger(self) -> Logger:
        """
        Retrieve the configured logger.

        :return: Logger object
        """
        return self.logger
