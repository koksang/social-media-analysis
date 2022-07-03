"""Logger module"""

import sys
from loguru import logger

LOG_FORMAT = "{time:YYYY-MM-DD HH:mm} | [{level: ^8}] {name: ^10}.{function: ^15}.{line: ^3} - {message: ^60}"
logger.remove(0)
logger.add(sys.stderr, format=LOG_FORMAT, colorize=True)
