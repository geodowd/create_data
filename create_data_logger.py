import logging

logger = logging.getLogger("create_data")
logger.setLevel(logging.DEBUG)

log_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

for handler in logger.handlers:
    logger.removeHandler(handler)

# add handler
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_format)
logger.addHandler(stream_handler)
