from functools import lru_cache

from covid19_il.logger.logger import Logger
from covid19_il.api_handler.api.api_data_global import ApiDataGlobal
from covid19_il.api_handler.api.api_data_il import ApiDataIL
from covid19_il.api_handler.iapi_handler import IAPIHandler
from covid19_il.api_handler.api_factory.api_enum import ApiEnum


class ApiFactory:
    """ API Factory for creating Types of API Clients.

    Attributes:
        None

    Methods:
        def create_api_client(required_api: ApiEnum): creates api's class instance.

    """

    @staticmethod
    @lru_cache(maxsize=2)
    def create_api_client(required_api: ApiEnum) -> IAPIHandler or None:
        """ Create Required API Client to fetch future Data

        Args:
            required_api(ApiEnum): enum type of desired api.
        Local:
            switch_case(dict): The button with all the properties set.
        Returns:
            IAPIHandler or None: api's class instance or None object.

        """
        if not isinstance(required_api, ApiEnum):
            raise TypeError("Not Api Enum Type")

        switch_case = {
            1: ApiDataIL(Logger().logger),
            2: ApiDataGlobal(Logger().logger),
            "default": None
        }

        return switch_case.get(required_api.value, switch_case['default'])
