"""Logger module"""

import sys
from loguru import logger

FORMAT = "{time:YYYY-MM-DD HH:mm} [{level: ^9}] {name}.{function}.{line} - {message}"
logger.remove(0)
logger.add(sys.stderr, format=FORMAT, colorize=True, level="INFO")
