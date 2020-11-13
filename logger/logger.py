import logging
from threading import Lock

"""
# DEBUG: Detailed information, typically of interest only when diagnosing problems.
## INFO: Confirmation that things are working as expected.
### WARNING: An indication that something unexpected happened, or indicative of some problem in the near future (e.g. ‘disk space low’). The software is still working as expected.
#### ERROR: Due to a more serious problem, the software has not been able to perform some function.
##### CRITICAL: A serious error, indicating that the program itself may be unable to continue running.
"""


class Logger:
    """ Universal Logger """
    _instance = None
    _logger = logging.getLogger('root')

    def __init__(self, logging_level: int = logging.DEBUG) -> None:
        self._logging_level = logging_level
        self._file_handler = logging.FileHandler(f".\\covid_19_il.log")
        self._stream_handler = logging.StreamHandler()
        self._formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
        self._logger_init()

    def __repr__(self) -> str:
        """ Returns Class Representation """
        return f"{self.__class__.__name__}({self._logging_level})"

    def __new__(cls, *args, **kwargs) -> 'Logger':
        """ Constructor's Double Check Lock for Handling a Singelton Instance """
        if not cls._instance:
            with Lock():
                if not cls._instance:
                    cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    @property
    def logger(self) -> logging.Logger:
        """ Returns Logger Component """
        return self._logger

    def _logger_init(self) -> None:
        """ Logger initialization: Level, Formatter and Handlers """
        self._logger.setLevel(self._logging_level)
        self._file_handler.setFormatter(self._formatter)
        self._stream_handler.setFormatter(self._formatter)
        self._logger.addHandler(self._file_handler)
        self._logger.addHandler(self._stream_handler)
