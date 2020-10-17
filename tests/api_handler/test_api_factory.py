import unittest

from covid19_il.api_handler.api_factory.api_factory import ApiFactory
from covid19_il.api_handler.api.api_data_il import ApiDataIL
from covid19_il.api_handler.api_factory.api_enum import ApiEnum

class TestApiFactory(unittest.TestCase):
    """ API Factory for creating Types of API Clients.

    Methods:
        def test_create_api_client(self): test api's class instance creation and lru_cache's behaviour as "singleton".

    """

    def setUp(self) -> None:
        """ Announce of starting the class's tests and initialize api's instances """
        print("testing ApiFactory Class...")
        self.api_data_1 = ApiFactory.create_api_client(ApiEnum.api_data_il)
        self.api_data_2 = ApiFactory.create_api_client(ApiEnum.api_data_il)
        self.api_data_3 = ApiFactory.create_api_client(ApiEnum.api_data_il)

    def tearDown(self) -> None:
        """ Announce of finishing the class's tests """
        print("finished testing ApiFactory Class...")

    def test_create_api_client(self) -> None:
        """ Test API's class instance creation and lru_cache's behaviour as "singleton". """
        # Checks if we successfully created api data il
        self.assertIsInstance(self.api_data_1, ApiDataIL)
        self.assertIsInstance(self.api_data_2, ApiDataIL)
        self.assertIsInstance(self.api_data_3, ApiDataIL)
        # Checks that the lru_cache works and returns the same object - singleton behavior
        self.assertEqual(id(self.api_data_1), id(self.api_data_2), id(self.api_data_3))
        # Checks lru_cache_functionality
        lru_cache_info = ApiFactory.create_api_client.cache_info()
        self.assertEqual(lru_cache_info.hits, 2)
        self.assertEqual(lru_cache_info.misses, 1)
        # Checks that exception gets raise when gets different type from ApiEnum
        with self.assertRaises(TypeError):
            self.api_data_1 = ApiFactory.create_api_client(1)
