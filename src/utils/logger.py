import sys
import pendulum
import logging
from logging import StreamHandler
from typing import Union
from coloredlogs import ColoredFormatter
from pathlib import Path


def get_time():
    return str(pendulum.now().format("YYYY-MM-DD+HH-mm-ss"))


def setup_logging(log_dir: Union[str, Path, None] = "outputs/logs", log_stdout=True, log_level="info", include_time=True, global_setup=True):
    handlers = []
    FORMAT = "%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s"
    log_level = logging.getLevelName(log_level.upper())

    if log_stdout and global_setup:
        stdout_handler = StreamHandler()
        stdout_handler.setLevel(log_level)
        formatter = ColoredFormatter(FORMAT)
        stdout_handler.setFormatter(formatter)
        handlers.append(stdout_handler)

    if log_dir is not None:
        filename = Path(log_dir) / "log.log"  if not include_time else Path(log_dir) / get_time() / "log.log"
        filename.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(filename, mode="a", encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(FORMAT)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
    
    if global_setup:
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        logging.basicConfig(
            level=logging.DEBUG,
            format=FORMAT,
            handlers=handlers,
            force=True,
        )
    else:
        if log_stdout:
            logging.basicConfig(
                level=log_level,
                format=FORMAT,
                stream=sys.stdout   
            )
        else:
            logging.basicConfig(
                level=log_level,
                format=FORMAT
            )

        return handlers
