#File: gateio_logger_setup.py

import os
import logging


class InfoFilter(logging.Filter):
    def filter(self, record):
        # Only pass records with level INFO (not WARNING, ERROR, etc.)
        return record.levelno == logging.INFO


def setup_logging():
    # Define the directory structure and log file paths
    log_dir = os.path.join('Gateio_Files', 'Gateio_Logs')
    info_log_file = os.path.join(log_dir, 'gateio_info.log')
    error_log_file = os.path.join(log_dir, 'gateio_error.log')

    # Check if the directory exists, if not, create it
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        print(f"Created directory: {log_dir}")

    # Set up logging
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Set logger to capture all levels

    # Suppress unnecessary logs from third-party libraries
    logging.getLogger("openai").setLevel(logging.WARNING)  # Suppress OpenAI client logs
    logging.getLogger("httpx").setLevel(logging.WARNING)  # Suppress httpx logs
    logging.getLogger("httpcore").setLevel(logging.WARNING)  # Suppress httpcore logs
    logging.getLogger("urllib3").setLevel(logging.WARNING)  # Suppress urllib3 logs
    logging.getLogger("http.client").setLevel(logging.WARNING)  # Suppress http.client logs
    logging.getLogger("http").setLevel(logging.WARNING)  # Suppress http logs

    # Create an INFO level log handler (logs INFO only)
    info_handler = logging.FileHandler(info_log_file)
    info_handler.setLevel(logging.INFO)
    info_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    info_handler.setFormatter(info_formatter)

    # Add filter to ensure only INFO messages are logged to gateio_info.log
    info_handler.addFilter(InfoFilter())

    # Create an ERROR level log handler (logs only ERROR and higher)
    error_handler = logging.FileHandler(error_log_file)
    error_handler.setLevel(logging.ERROR)
    error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    error_handler.setFormatter(error_formatter)

    # Add handlers to the logger
    logger.addHandler(info_handler)
    logger.addHandler(error_handler)
