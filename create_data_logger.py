import logging

logger = logging.getLogger("create_data")
logger.setLevel(logging.INFO)

log_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

for handler in logger.handlers:
    handler.setFormatter(log_format)
