from covid19_il.tests.data_handler.data_handler_tests_utils import DataHandlerTestsUtils

from covid19_il.data_handler.data_handlers.area import Area
from covid19_il.data_handler.enums.area_event import AreaEvent
from covid19_il.data_handler.enums.resource_id import ResourceId


class TestArea(DataHandlerTestsUtils):
    """ Tests for Area Data Handler Class.

    Methods:
        setUp(self): Announce of starting the class's tests, initialize & verify area data handler's instance.
        tearDown(self): announce of finishing the class's tests
        test_get_data_by_event_type(self): Tests same logic behavior just different area type which affects the results.
        test_get_accumulated_tested_by_town(self): Tests which have the same logic for 3 methods only results are
            different.
    """

    def setUp(self) -> None:
        """ Announce of starting the class's tests, initialize & verify area data handler's instance """
        print("testing Area Class...")
        self.data_handler_1 = self._init_mocked_data_handler(json_file_path="json_files/area_mocked_data.json",
                                                             resource_id_enum=ResourceId.AREA_RESOURCE_ID)
        self._check_base_step_of_all_methods(data_handler=self.data_handler_1, class_type=Area)

    def test_get_data_by_event_type(self) -> None:
        """ Tests same logic behavior just different area type which affects the results """
        # Check equality of results gets from the method of the mocked data
        data = self.data_handler_1.get_data_by_event_type(AreaEvent.NEW_HOSPITALIZED_ON_DATE)
        self.assertEqual(data, {('אופקים', '0'): 'FALSE', ('מזכרת בתיה', '0'): 'FALSE', ('ראש פינה', '0'): 'FALSE'})

    def test_get_accumulated_tested_by_town(self) -> None:
        """ Tests which have the same logic for 3 methods only results are different """
        # Check equality of results gets from the method of the mocked data by de/ascending order
        data = self.data_handler_1.get_accumulated_tested_by_town(ascending_order=True)
        self.assertEqual(data, {'אופקים': 3261, 'מזכרת בתיה': 395025, 'ראש פינה': 38937})
        data = self.data_handler_1.get_accumulated_tested_by_town()#False
        self.assertEqual(data, {'ראש פינה': 38937, 'מזכרת בתיה': 395025, 'אופקים': 3261})
