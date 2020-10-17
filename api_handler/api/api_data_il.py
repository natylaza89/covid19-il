import os
from dotenv import load_dotenv
import requests
from requests.exceptions import (HTTPError, SSLError, InvalidURL, ConnectTimeout, ConnectionError, Timeout,
                                 RequestException, MissingSchema)
from typing import Dict

from covid19_il.api_handler.iapi_handler import IAPIHandler
from covid19_il.logger.logger import Logger
from covid19_il.data_handler.enums.resource_id import ResourceId


class ApiDataIL(IAPIHandler):
    """ API Client for Israeli Government Covid19 Data

    Attributes:
        _logger(Logger.logger): Api Data IL instance's actions logger.
        _base_url(str): base url of Israel Data gov API.
        _url_query(str): final url query for http get request
        _json_data(dict): http get request's results dictionary
        _request_status(int): http get request's results status

    Todo:
        * storing next & previous url query with offset jumps

    """

    def __init__(self, logger) -> None:
        load_dotenv()
        self._logger = logger
        self._base_url = os.getenv("API_DATA_GOV_IL_URL")
        self._url_query = None
        self._json_data = None
        self._request_status = None
        self._logger.info(f"Created ApiDataIL: logger({self._logger})")

    def __repr__(self) -> str:
        """ Returns Class Representation """
        return f"{self.__class__.__name__}({self._logger})"

    def __bool__(self) -> bool:
        """ Returns Class Truth Value """
        return self._base_url is not None and len(self._base_url) == 19

    @property
    def logger(self) -> Logger.logger:
        """ Logger.logger: Returns an instance of a logger """
        return self._logger

    @property
    def base_url(self) -> str:
        """ str: api's base url """
        return self._base_url

    @property
    def url_query(self) -> str:
        """ str: Returns a string of the final url query for http get request.
                 if the url is None or not a string then raise TypeError exception. """
        return self._url_query

    @url_query.setter
    def url_query(self, required_url_query: str) -> None:
        if required_url_query is not None and isinstance(required_url_query, str):
            self._url_query = required_url_query
        else:
            self._logger.exception(f"Wrong Type - {type(required_url_query)} is not a string")
            raise TypeError("Wrong Type - not a string for a query")

    @property
    def json_data(self) -> Dict:
        """ dict: Returns a dictionary of http get request's results.
                 if the result is None or not a dictionary then raise TypeError exception. """
        return self._json_data

    @json_data.setter
    def json_data(self, fetched_json_data: dict) -> None:
        if fetched_json_data is not None and isinstance(fetched_json_data, dict):
            self._json_data = fetched_json_data
        else:
            self._logger.exception(f"Wrong Type - {type(fetched_json_data)} is not a json data dict")
            raise TypeError("Wrong Type - not a dict for a json data")

    @property
    def request_status(self) -> str:
        """ int: Returns an integer of http get request's results status. """
        return self.request_status

    def _get_request(self) -> int:
        """ Get request implementation - get request from IL Data Gov, save Data and return request's status code.
        Note:
            private method which get called by get_data_by_resource_id's method.
        Args:
            None.

        Returns:
            self._request_status(int): http get request's status code.

        Raises:
            HTTPError, SSLError, InvalidURL, ConnectTimeout, ConnectionError, Timeout, RequestException,
            MissingSchema: concrete error which can occurred.
            Exception: general error which can occurred.
         """

        try:
            request_result = requests.get(self._url_query)
            self._request_status = request_result.status_code
            if request_result.ok:
                self._json_data = request_result.json()
        except (HTTPError, SSLError, InvalidURL, ConnectTimeout, ConnectionError, Timeout, RequestException,
                MissingSchema) as concrete_error:
            self._logger.exception(concrete_error)
        except Exception as general_error:
            self._logger.exception(general_error)
        finally:
            return self._request_status

    def _build_url_query_by_parameters(self,
                                       enum_resource_id: ResourceId,
                                       limit: int,
                                       offset: int,
                                       include_total: bool = False,
                                       query: str = None) -> None:
        """ Helper Method of Building URL Query for future http get request via Rest API
        Note:
            private method which get called by get_data_by_resource_id's method.
        Args:
            enum_resource_id(ResourceId): data resource's id.
            limit(int): result's limitation.
            offset(int): result's offset.
            include_total(bool): include total amount.
            query(str) = None: additional parameters as query string.

        Returns:
            None.
        """

        self._url_query = f"{self._base_url}/api/3/action/datastore_search?" \
                          f"resource_id={os.getenv(enum_resource_id.name)}"
        if limit:
            self._url_query += f"&limit={limit}"
        if offset:
            self._url_query += f"&offset={offset}"
        if include_total:
            self._url_query += f"&include_total={include_total}"
        if query:
            self._url_query += f"&q={query}"

        self._logger.info(f"url_query = {self._url_query}")

    def get_data_by_resource_id(self,
                                enum_resource_id: ResourceId,
                                limit: int = 0,
                                offset: int = 0,
                                include_total: bool = False,
                                query: str = None) -> Dict:
        """ Get data from specific data resource.
        Note:
            private method which get called by get_data_by_resource_id's method.
        Args:
            enum_resource_id(ResourceId): data resource's id.
            limit(int): result's limitation.
            offset(int): result's offset.
            include_total(bool): include total amount.
            query(str) = None: additional parameters as query string.

        Returns:
            self._json_data(dict): returns a dictionary of get request's result.
        """
        self._build_url_query_by_parameters(enum_resource_id, limit, offset, include_total, query)
        status_code = self._get_request()
        self._logger.debug(f"status code is {status_code}, and value returned is equal to object attribute: "
                           f"{status_code == self._request_status}")

        return self._json_data
