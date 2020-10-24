import unittest
import json

from covid19_il.data_handler.data_handlers_factory.data_handler_factory import DataHandlerFactory
from covid19_il.data_handler.data_handlers.area import Area
from covid19_il.data_handler.enums.area_event import AreaEvent
from covid19_il.data_handler.enums.resource_id import ResourceId


class TestArea(unittest.TestCase):
    """ Data Handler Factory for creating Types of resource data handlers.

    Methods:
        def setUp(self): Announce of starting the class's tests, initialize & verify area data handler's instance.
        def tearDown(self): announce of finishing the class's tests
        test_get_data_by_event_type(self): Tests same logic behavior just different area type which affects the results.
        test_get_accumulated_tested_by_town(self): Tests which have the same logic for 3 methods only results are
            different.
    """

    def setUp(self) -> None:
        """ Announce of starting the class's tests, initialize & verify area data handler's instance """
        print("testing Area Class...")
        with open("json_files/area_mocked_data.json") as json_file:
            mocked_json_data = json.load(json_file)
            self.data_handler_1 = DataHandlerFactory.get_instance(ResourceId.AREA_RESOURCE_ID, mocked_json_data)
        self.assertIsInstance(self.data_handler_1, Area)

    def tearDown(self) -> None:
        """ Announce of finishing the class's tests """
        print("finished testing Area Class...")

    def test_get_data_by_event_type(self) -> None:
        """ Tests same logic behavior just different area type which affects the results """
        data_dict = self.data_handler_1._get_clean_copy_df_data()
        # Check unequally of different memory addresses of copied df
        self.assertNotEqual(id(data_dict), id(self.data_handler_1.df))
        # Check equality of results gets from the method of the mocked data
        data = self.data_handler_1.get_data_by_event_type(AreaEvent.NEW_HOSPITALIZED_ON_DATE)
        self.assertEqual(data, {('אופקים', '0'): 'FALSE', ('מזכרת בתיה', '0'): 'FALSE', ('ראש פינה', '0'): 'FALSE'})

    def test_get_accumulated_tested_by_town(self) -> None:
        """ Tests which have the same logic for 3 methods only results are different """
        data_dict = self.data_handler_1._get_clean_copy_df_data()
        # Check unequally of different memory addresses of copied df
        self.assertNotEqual(id(data_dict), id(self.data_handler_1.df))
        # Check equality of results gets from the method of the mocked data by de/ascending order
        data = self.data_handler_1.get_accumulated_tested_by_town(ascending_order=True)
        self.assertEqual(data, {'אופקים': 3261, 'מזכרת בתיה': 395025, 'ראש פינה': 38937})
        data = self.data_handler_1.get_accumulated_tested_by_town()#False
        self.assertEqual(data, {'ראש פינה': 38937, 'מזכרת בתיה': 395025, 'אופקים': 3261})
