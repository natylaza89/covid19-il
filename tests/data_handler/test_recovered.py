from collections import defaultdict

from tests.data_handler.data_handler_tests_utils import DataHandlerTestsUtils
from covid19_il.data_handler.data_handlers.recovered import Recovered
from covid19_il.data_handler.enums.resource_id import ResourceId


class TestRecovered(DataHandlerTestsUtils):
    """ Tests for Recovered Data Handler Class.

    Methods:
        setUp(self): Announce of starting the class's tests, initialize & verify cities data handler's instance.
        test_test_indication(self): Tests results data & type of test indication.
        test_days_from_pos_to_recovery_stats(self): Tests results data & type of test days from pos to recovery stats.
        test_total_tests_count(self): Tests results data & type of total tests count.

    """

    def setUp(self) -> None:
        """ Announce of starting the class's tests, initialize & verify Recovered data handler's instance """
        print("testing Recovered Class...")
        self.data_handler_1 = self._init_mocked_data_handler(json_file_path="json_files/recovered_mocked_data.json",
                                                             resource_id_enum=ResourceId.RECOVERED_RESOURCE_ID)
        self._check_base_step_of_all_methods(data_handler=self.data_handler_1, class_type=Recovered)

    def test_test_indication(self) -> None:
        """ Tests results data & type of test indication """
        # Get Data
        data = self.data_handler_1.test_indication()
        results = defaultdict(None,
                              {'Contact with confirmed': defaultdict(None,
                                     {'זכר': defaultdict(int, {'0-19': 9, '20-29': 2, '30-39': 1, '50-59': 3, '60+': 1}),
                                      'נקבה': defaultdict(int, {'0-19': 10, '20-29': 2, '30-39': 1, '40-49': 2, '50-59': 4, '60+': 4})}),
                               'Other': defaultdict(None,
                                    {'זכר': defaultdict(int, {'0-19': 9, '20-29': 4, '30-39': 6, '40-49': 1, '50-59': 5, '60+': 6}),
                                     'נקבה': defaultdict(int, {'0-19': 18, '20-29': 4, '30-39': 3, '50-59': 1, '60+': 4})})})
        # Data Validation
        self._test_three_level_depth_nested_dictionaries(data, results)

    def test_days_from_pos_to_recovery_stats(self) -> None:
        """ Tests results data & type of test days from pos to recovery stats """
        # Get Data
        data = self.data_handler_1.days_from_pos_to_recovery_stats()
        results = {'min': 1, 'max': 9, 'mean': 2.29}
        # Data Validation
        self._test_one_level_depth_dictionary(data, results)

    def test_total_tests_count(self) -> None:
        """ Tests results data & type of total tests count  """
        # Get Data
        data = self.data_handler_1.total_tests_count()
        results = defaultdict(None,
                              {'0-19': defaultdict(int, {'זכר': 19, 'נקבה': 29}),
                               '50-59': defaultdict(int, {'זכר': 8, 'נקבה': 6}),
                               '30-39': defaultdict(int, {'זכר': 7, 'נקבה': 4}),
                               '60+': defaultdict(int, {'זכר': 7, 'נקבה': 11}),
                               '20-29': defaultdict(int, {'זכר': 6, 'נקבה': 6}),
                               '40-49': defaultdict(int, {'זכר': 1, 'נקבה': 2})})
        # Data Validation
        self._test_two_level_depth_nested_dictionaries(data, results)
