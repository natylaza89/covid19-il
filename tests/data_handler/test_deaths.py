from collections import defaultdict

from tests.data_handler.data_handler_tests_utils import DataHandlerTestsUtils
from covid19_il.data_handler.data_handlers.deaths import Deaths
from covid19_il.data_handler.enums.resource_id import ResourceId


class TestDeaths(DataHandlerTestsUtils):
    """ Tests for Deaths Data Handler Class.

    Methods:
        setUp(self): Announce of starting the class's tests, initialize & verify cities data handler's instance.
        test_amount_of_deaths(self): Tests amount of death's results validity.
        test_amount_of_ventilated(self): Tests amount of ventilated's results data & type.
        test_time_between_positive_and_hospitalization(self): Tests test_time_between_positive_and_hospitalization of
            death's results validity. Same logic for both length_of_hospitalization & time_between_positive_and_death
            methods.
    """

    def setUp(self) -> None:
        """ Announce of starting the class's tests, initialize & verify deaths data handler's instance """
        print("testing Deaths Class...")
        self.data_handler_1 = self._init_mocked_data_handler(json_file_path="json_files/deaths_mocked_data.json",
                                                             resource_id_enum=ResourceId.DEATHS_DATA_RESOURCE_ID)
        self._check_base_step_of_all_methods(data_handler=self.data_handler_1, class_type=Deaths)

    def test_amount_of_deaths(self) -> None:
        """ Tests amount of death's results validity """
        # Get Data
        data = self.data_handler_1.amount_of_deaths()
        results = defaultdict(None,
                             {'זכר': defaultdict(int, {'75-84': 97, '65-74': 93, '<65': 62, '85+': 62}),
                              'נקבה': defaultdict(int, {'85+': 63, '75-84': 52, '65-74': 41, '<65': 30})})
        # Data Validation
        self._test_two_level_depth_nested_dictionaries(data, results)

    def test_amount_of_ventilated(self) -> None:
        """ Tests amount of ventilated's results data & type  """
        # Get Data
        data = self.data_handler_1.amount_of_ventilated()
        results = defaultdict(None,
                              {'זכר': defaultdict(None, {'65-74': defaultdict(int, {'0': 28, '1': 65}),
                                                         '75-84': defaultdict(int, {'0': 50, '1': 47}),
                                                         '85+': defaultdict(int, {'0': 38, '1': 23, 'NULL': 1}),
                                                         '<65': defaultdict(int, {'0': 18, '1': 43, 'NULL': 1})}),
                               'נקבה': defaultdict(None, {'65-74': defaultdict(int, {'0': 20, '1': 21}),
                                                          '75-84': defaultdict(int, {'0': 32, '1': 20}),
                                                          '85+': defaultdict(int, {'0': 51, '1': 12}),
                                                          '<65': defaultdict(int, {'0': 10, '1': 20})})})

        # Data Validation
        self._test_three_level_depth_nested_dictionaries(data, results)

    def test_time_between_positive_and_hospitalization(self) -> None:
        """ Tests  test_time_between_positive_and_hospitalization of death's results validity.
            Same logic for both length_of_hospitalization & time_between_positive_and_death methods """
        # Get Data
        data = self.data_handler_1.time_between_positive_and_hospitalization()
        results = defaultdict(None,
                              {'65-74': defaultdict(int, {'0': 44, '1': 24, '-1': 13, '3': 11, '5': 7, '2': 6, '7': 5, '4': 4, '6': 4, '8': 3, '-2': 2, '-9': 2, '-14': 1, '-15': 1, '-21': 1, '-22': 1, '-5': 1, '10': 1, '11': 1, '17': 1, '9': 1}),
                               '75-84': defaultdict(int, {'0': 59, '1': 25, '2': 18, '-1': 9, '4': 7, '3': 5, '6': 5, '7': 4, '9': 4, '10': 3, '-6': 2, '5': 2, '-2': 1, '-26': 1, '-3': 1, '-5': 1, '-9': 1, '11': 1}),
                               '85+': defaultdict(int, {'0': 49, '1': 25, '-1': 9, '2': 6, '6': 6, '9': 6, '10': 3, '3': 3, '4': 3, '5': 3, '7': 2, '8': 2, '-11': 1, '-12': 1, '-15': 1, '-2': 1, '-8': 1, '11': 1, '12': 1, 'NULL': 1}),
                               '<65': defaultdict(int, {'0': 45, '1': 10, '4': 5, '8': 5, '2': 4, '5': 4, '9': 4, '6': 3, '-1': 2, '3': 2, '-17': 1, '-4': 1, '-5': 1, '10': 1, '12': 1, '20': 1, '7': 1, 'NULL': 1})
                               })
        # Data Validation
        self._test_two_level_depth_nested_dictionaries(data, results)
