from logging import (
    Logger,
    basicConfig,
    Formatter,
    StreamHandler,
    getLogger,
    INFO,
    ERROR,
    NOTSET,
)
from os import getenv
from typing import Any

level = "INFO"


class CustomLogger(Logger):
    def __init__(self, name: str, level: Any = NOTSET):
        super().__init__(name, level)

        basicConfig(level=NOTSET)

        log_format = "%(asctime)s [%(levelname)s] %(message)s"
        formatter = Formatter(log_format)

        self.stream_handler = StreamHandler()
        self.stream_handler.setFormatter(formatter)

        self.stream_handler.setLevel(level)

        self.addHandler(self.stream_handler)

        getLogger("urllib3").setLevel(INFO)
        getLogger("google").setLevel(INFO)
        getLogger("botocore").setLevel(ERROR)
        getLogger("pyathena").setLevel(ERROR)
        getLogger("boto3").setLevel(ERROR)
        getLogger("botocore").setLevel(ERROR)
        getLogger("s3transfer").setLevel(ERROR)


# Instantiate an object of CustomLogger class
logger = CustomLogger(__name__, level=level)
