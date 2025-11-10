import logging


class CustomFormatter(logging.Formatter):

    grey = "\x1b[38;20m"
    blue = "\x1b[34;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = (
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"
    )

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: blue + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class DataLogger(metaclass=Singleton):
    logger: logging.Logger = None

    def setup_logger(self, name: str, level: int = logging.INFO) -> logging.Logger:
        if not self.logger:
            logger = logging.getLogger(name)
            logger.setLevel(level)
            ch = logging.StreamHandler()
            ch.setLevel(level)
            formatter = CustomFormatter()
            ch.setFormatter(formatter)
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)
            logger.addHandler(ch)
            self.logger = logger
        return self.logger
