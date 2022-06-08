import sys
from loguru import logger as LOGGER

LOGGER_FORMAT = "{time:YYYY-MM-DD HH:mm} | [{level: ^8}] {name: ^10}.{function: ^15}.{line: ^3} - {message: ^60}"
LOGGER.remove(0)
LOGGER.add(sys.stderr, format=LOGGER_FORMAT, colorize=True)
