import unittest

from covid19_il.data_handler.data_handlers_factory.data_handler_factory import DataHandlerFactory
from covid19_il.data_handler.data_handlers.area import Area
from covid19_il.data_handler.enums.resource_id import ResourceId


class TestDataHandlerFactory(unittest.TestCase):
    """ Data Handler Factory for creating Types of resource data handlers.

    Methods:
        def setUp(self): announce of starting the class's tests and initialize data handler's instances
        def tearDown(self): announce of finishing the class's tests
        def get_instance(cls, required_resource_id: ResourceId, json_data: dict = None): test data handlers class
         instance creation and memoization dictionary's behaviour as "singleton".

    """

    def setUp(self) -> None:
        """ Announce of starting the class's tests and initialize api's instances """
        print("testing ApiFactory Class...")
        mocked_json_data = {"result": {"records": {}}}
        self.data_handler_1 = DataHandlerFactory.get_instance(ResourceId.AREA_RESOURCE_ID, mocked_json_data)
        self.data_handler_2 = DataHandlerFactory.get_instance(ResourceId.AREA_RESOURCE_ID, mocked_json_data)
        self.data_handler_3 = DataHandlerFactory.get_instance(ResourceId.AREA_RESOURCE_ID, mocked_json_data)

    def tearDown(self) -> None:
        """ Announce of finishing the class's tests """
        print("finished testing ApiFactory Class...")

    def test_get_instance(self) -> None:
        """ Test API's class instance creation and lru_cache's behaviour as "singleton". """
        # Checks if we successfully created Area data handler
        self.assertIsInstance(self.data_handler_1, Area)
        self.assertIsInstance(self.data_handler_2, Area)
        self.assertIsInstance(self.data_handler_3, Area)
        # Checks that the dictionary works and returns the same object - memoization
        self.assertEqual(id(self.data_handler_1), id(self.data_handler_2), id(self.data_handler_3))
