import unittest
from unittest.mock import patch
import os
import json

from covid19_il.api_handler.api_factory.api_factory import ApiFactory
from covid19_il.api_handler.api_factory.api_enum import ApiEnum
from covid19_il.data_handler.enums.resource_id import ResourceId


class TestApiDataIL(unittest.TestCase):
    """ API Factory for creating Types of API Clients.

     Methods:
         def setUp(self): announce of starting the class's tests and initialize api's instances
         def tearDown(self): announce of finishing the class's tests
         def test_create_api_client(self): test api's class instance creation and lru_cache's behaviour as "singleton".

     """

    def setUp(self) -> None:
        """ Announce of starting the class's tests and initialize api's instances """
        print("testing ApiDataIL Class...")
        self.api_data_1 = ApiFactory.create_api_client(ApiEnum.api_data_il)

    def tearDown(self) -> None:
        """ Announce of finishing the class's tests """
        print("finished testing ApiDataIL Class...")
        del self.api_data_1

    def test_get_data_by_resource_id(self) -> None:
        # url query build
        mocked_url_query = f"{self.api_data_1.base_url}/api/3/action/datastore_search?" \
                           f"resource_id={os.getenv(ResourceId.AREA_RESOURCE_ID.name)}&limit=5"

        # mocked get request
        with patch('covid19_il.api_handler.api.api_data_il.requests.get') as mocked_get:
            # Check a successful http get request
            with open('mocked_api_data.txt') as mocked_result:
                mocked_get.return_value.content = json.load(mocked_result)
            mocked_get.return_value.ok = True
            mocked_get.return_value.status_code = 200

            response = self.api_data_1.get_data_by_resource_id(enum_resource_id=ResourceId.AREA_RESOURCE_ID, limit=5)
            mocked_get.assert_called_with(mocked_url_query)
            self.assertEqual(response, self.api_data_1.json_data)
            self.assertEqual(mocked_get.return_value.status_code, self.api_data_1.request_status)

            # Check a failure http get request
            mocked_get.return_value.ok = False
            mocked_get.return_value.status_code = 404
            mocked_url_query = f"{self.api_data_1.base_url}/api/3/action/datastore_search?" \
                               f"resource_id={os.getenv(ResourceId.DEATHS_DATA_RESOURCE_ID.name)}&limit=5"

            response = self.api_data_1.get_data_by_resource_id(enum_resource_id=ResourceId.DEATHS_DATA_RESOURCE_ID,
                                                               limit=5)
            mocked_get.assert_called_with(mocked_url_query)
            self.assertEqual(response, self.api_data_1.json_data)
            self.assertEqual(mocked_get.return_value.status_code, self.api_data_1.request_status)
