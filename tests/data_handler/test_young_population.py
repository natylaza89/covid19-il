from collections import defaultdict

from tests.data_handler.data_handler_tests_utils import DataHandlerTestsUtils
from covid19_il.data_handler.data_handlers.young_population import YoungPopulation
from covid19_il.data_handler.enums.resource_id import ResourceId


class TestYoungPopulation(DataHandlerTestsUtils):
    """ Tests for Young Population Data Handler Class.

    Methods:
        setUp(self): Announce of starting the class's tests, initialize & verify Young Population data handler's
            instance.
        test_cases_statistics_by_region(self): Validate result's data & types of cases_statistics_by_region.
            cases_statistics_by_age_group & cases_statistics_by_first_week_day methods has same logic.
        test_total_cases_statistics(self): Validate result's data & types of total_cases_statistics.

    """

    def setUp(self) -> None:
        """ Announce of starting the class's tests, initialize & verify deaths data handler's instance """
        print("testing Young Population Morbidity Class...")
        self.data_handler_1 = \
            self._init_mocked_data_handler(json_file_path="json_files/young_population_mocked_data.json",
                                           resource_id_enum=ResourceId.YOUNG_POPULATION_RESOURCE_ID)
        self._check_base_step_of_all_methods(data_handler=self.data_handler_1, class_type=YoungPopulation)

    def test_cases_statistics_by_region(self) -> None:
        """ Validate result's data & types of cases_statistics_by_region.
            cases_statistics_by_age_group & cases_statistics_by_first_week_day methods has same logic. """
        # Get Data
        data = self.data_handler_1.cases_statistics_by_region()
        results = defaultdict(None,
                              {'מחוז מרכז':
                                   {'weekly_tests_num': {'min': 45, 'max': 491, 'mean': 172.66666666666666, 'total': 2072},
                                    'weekly_newly_tested': {'min': 45, 'max': 451, 'mean': 162.5, 'total': 1950},
                                    'weekly_cases': {'min': 0, 'max': 37, 'mean': 15.5, 'total': 186}},
                               'מחוז צפון':
                                   {'weekly_tests_num': {'min': 16, 'max': 188, 'mean': 61.0, 'total': 793},
                                    'weekly_newly_tested': {'min': 16, 'max': 182, 'mean': 58.38461538461539, 'total': 759},
                                    'weekly_cases': {'min': 0, 'max': 14, 'mean': 11.846153846153847, 'total': 154}},
                               'מחוז חיפה':
                                   {'weekly_tests_num': {'min': 14, 'max': 125, 'mean': 37.38461538461539, 'total': 486},
                                    'weekly_newly_tested': {'min': 14, 'max': 115, 'mean': 35.30769230769231, 'total': 459},
                                    'weekly_cases': {'min': 0, 'max': 14, 'mean': 6.461538461538462, 'total': 84}},
                               'מחוז אשקלון':
                                   {'weekly_tests_num': {'min': 14, 'max': 79, 'mean': 34.0, 'total': 442},
                                    'weekly_newly_tested': {'min': 14, 'max': 76, 'mean': 32.07692307692308, 'total': 417},
                                    'weekly_cases': {'min': 0, 'max': 14, 'mean': 11.846153846153847, 'total': 154}},
                               'מחוז ירושלים':
                                   {'weekly_tests_num': {'min': 55, 'max': 217, 'mean': 106.6923076923077, 'total': 1387},
                                    'weekly_newly_tested': {'min': 55, 'max': 198, 'mean': 101.61538461538461, 'total': 1321},
                                    'weekly_cases': {'min': 14, 'max': 23, 'mean': 14.692307692307692, 'total': 191}},
                               'מחוז תל אביב':
                                   {'weekly_tests_num': {'min': 27, 'max': 256, 'mean': 97.58333333333333, 'total': 1171},
                                    'weekly_newly_tested': {'min': 25, 'max': 229, 'mean': 88.33333333333333, 'total': 1060},
                                    'weekly_cases': {'min': 0, 'max': 28, 'mean': 11.833333333333334, 'total': 142}},
                               'לא ידוע':
                                   {'weekly_tests_num': {'min': 0, 'max': 14, 'mean': 5.833333333333333, 'total': 70},
                                    'weekly_newly_tested': {'min': 0, 'max': 14, 'mean': 5.833333333333333, 'total': 70},
                                    'weekly_cases': {'min': 0, 'max': 0, 'mean': 0.0, 'total': 0}},
                               'מחוז דרום':
                                   {'weekly_tests_num': {'min': 14, 'max': 119, 'mean': 44.75, 'total': 537},
                                    'weekly_newly_tested': {'min': 14, 'max': 112, 'mean': 43.333333333333336, 'total': 520},
                                    'weekly_cases': {'min': 0, 'max': 14, 'mean': 7.0, 'total': 84}}})

        # Data Validation
        self._test_three_level_depth_nested_dictionaries(data, results)

    def test_total_cases_statistics(self) -> None:
        """ Validate result's data & types of total_cases_statistics. """
        # Get Data
        data = self.data_handler_1.total_cases_statistics()
        results = defaultdict(None,
                              {'2020-03-15':
                                   defaultdict(None,
                                               {'לא ידוע':
                                                    defaultdict(dict,
                                                                {'0-2': {'weekly_tests_num': 14, 'weekly_newly_tested': 14, 'weekly_cases': 0},
                                                                 '12-14': {'weekly_tests_num': 0, 'weekly_newly_tested': 0, 'weekly_cases': 0},
                                                                 '15-17': {'weekly_tests_num': 0, 'weekly_newly_tested': 0, 'weekly_cases': 0},
                                                                 '18-20': {'weekly_tests_num': 14, 'weekly_newly_tested': 14, 'weekly_cases': 0},
                                                                 '3-5': {'weekly_tests_num': 14, 'weekly_newly_tested': 14, 'weekly_cases': 0},
                                                                 '6-8': {'weekly_tests_num': 0, 'weekly_newly_tested': 0, 'weekly_cases': 0},
                                                                 '9-11': {'weekly_tests_num': 0, 'weekly_newly_tested': 0, 'weekly_cases': 0}}),
                                                'מחוז אשקלון': defaultdict(dict,
                                                                           {'0-2': {'weekly_tests_num': 33, 'weekly_newly_tested': 30, 'weekly_cases': 0},
                                                                            '12-14': {'weekly_tests_num': 14, 'weekly_newly_tested': 14, 'weekly_cases': 14},
                                                                            '15-17': {'weekly_tests_num': 14, 'weekly_newly_tested': 14, 'weekly_cases': 0},
                                                                            '18-20': {'weekly_tests_num': 41, 'weekly_newly_tested': 37, 'weekly_cases': 14},
                                                                            '3-5': {'weekly_tests_num': 16, 'weekly_newly_tested': 15, 'weekly_cases': 14},
                                                                            '6-8': {'weekly_tests_num': 14, 'weekly_newly_tested': 14, 'weekly_cases': 14},
                                                                            '9-11': {'weekly_tests_num': 17, 'weekly_newly_tested': 16, 'weekly_cases': 14}}),
                                                'מחוז דרום': defaultdict(dict,
                                                                         {'0-2': {'weekly_tests_num': 30, 'weekly_newly_tested': 30, 'weekly_cases': 0},
                                                                          '12-14': {'weekly_tests_num': 14, 'weekly_newly_tested': 14, 'weekly_cases': 0},
                                                                          '15-17': {'weekly_tests_num': 16, 'weekly_newly_tested': 15, 'weekly_cases': 0},
                                                                          '18-20': {'weekly_tests_num': 43, 'weekly_newly_tested': 43, 'weekly_cases': 14},
                                                                          '3-5': {'weekly_tests_num': 32, 'weekly_newly_tested': 32, 'weekly_cases': 0},
                                                                          '6-8': {'weekly_tests_num': 14, 'weekly_newly_tested': 14, 'weekly_cases': 0},
                                                                          '9-11': {'weekly_tests_num': 16, 'weekly_newly_tested': 16, 'weekly_cases': 0}}),
                                                'מחוז חיפה': defaultdict(dict,
                                                                         {'0-2': {'weekly_tests_num': 41, 'weekly_newly_tested': 41, 'weekly_cases': 0},
                                                                          '12-14': {'weekly_tests_num': 14, 'weekly_newly_tested': 14, 'weekly_cases': 0},
                                                                          '15-17': {'weekly_tests_num': 14, 'weekly_newly_tested': 14, 'weekly_cases': 0},
                                                                          '18-20': {'weekly_tests_num': 40, 'weekly_newly_tested': 40, 'weekly_cases': 14},
                                                                          '3-5': {'weekly_tests_num': 19, 'weekly_newly_tested': 18, 'weekly_cases': 0},
                                                                          '6-8': {'weekly_tests_num': 18, 'weekly_newly_tested': 18, 'weekly_cases': 0},
                                                                          '9-11': {'weekly_tests_num': 14, 'weekly_newly_tested': 14, 'weekly_cases': 0}}),
                                                'מחוז ירושלים': defaultdict(dict,
                                                                            {'0-2': {'weekly_tests_num': 115, 'weekly_newly_tested': 115, 'weekly_cases': 14},
                                                                             '12-14': {'weekly_tests_num': 62, 'weekly_newly_tested': 59, 'weekly_cases': 14},
                                                                             '15-17': {'weekly_tests_num': 59, 'weekly_newly_tested': 59, 'weekly_cases': 14},
                                                                             '18-20': {'weekly_tests_num': 110, 'weekly_newly_tested': 106, 'weekly_cases': 14},
                                                                             '3-5': {'weekly_tests_num': 77, 'weekly_newly_tested': 76, 'weekly_cases': 14},
                                                                             '6-8': {'weekly_tests_num': 64, 'weekly_newly_tested': 64, 'weekly_cases': 14},
                                                                             '9-11': {'weekly_tests_num': 55, 'weekly_newly_tested': 55, 'weekly_cases': 14}}),
                                                'מחוז מרכז': defaultdict(dict,
                                                                         {'0-2': {'weekly_tests_num': 109, 'weekly_newly_tested': 107, 'weekly_cases': 14},
                                                                          '12-14': {'weekly_tests_num': 75, 'weekly_newly_tested': 72, 'weekly_cases': 14},
                                                                          '15-17': {'weekly_tests_num': 101, 'weekly_newly_tested': 86, 'weekly_cases': 14},
                                                                          '18-20': {'weekly_tests_num': 180, 'weekly_newly_tested': 168, 'weekly_cases': 14},
                                                                          '3-5': {'weekly_tests_num': 86, 'weekly_newly_tested': 86, 'weekly_cases': 14},
                                                                          '6-8': {'weekly_tests_num': 77, 'weekly_newly_tested': 77, 'weekly_cases': 14},
                                                                          '9-11': {'weekly_tests_num': 45, 'weekly_newly_tested': 45, 'weekly_cases': 0}}),
                                                'מחוז צפון': defaultdict(dict,
                                                                         {'0-2': {'weekly_tests_num': 71, 'weekly_newly_tested': 69, 'weekly_cases': 14},
                                                                          '12-14': {'weekly_tests_num': 18, 'weekly_newly_tested': 18, 'weekly_cases': 0},
                                                                          '15-17': {'weekly_tests_num': 18, 'weekly_newly_tested': 18, 'weekly_cases': 14},
                                                                          '18-20': {'weekly_tests_num': 61, 'weekly_newly_tested': 61, 'weekly_cases': 14},
                                                                          '3-5': {'weekly_tests_num': 45, 'weekly_newly_tested': 44, 'weekly_cases': 14},
                                                                          '6-8': {'weekly_tests_num': 19, 'weekly_newly_tested': 19, 'weekly_cases': 0},
                                                                          '9-11': {'weekly_tests_num': 16, 'weekly_newly_tested': 16, 'weekly_cases': 14}}),
                                                'מחוז תל אביב': defaultdict(dict,
                                                                            {'0-2': {'weekly_tests_num': 94, 'weekly_newly_tested': 81, 'weekly_cases': 0},
                                                                             '12-14': {'weekly_tests_num': 27, 'weekly_newly_tested': 25, 'weekly_cases': 0},
                                                                             '15-17': {'weekly_tests_num': 28, 'weekly_newly_tested': 28, 'weekly_cases': 0},
                                                                             '18-20': {'weekly_tests_num': 95, 'weekly_newly_tested': 80, 'weekly_cases': 14},
                                                                             '3-5': {'weekly_tests_num': 38, 'weekly_newly_tested': 34, 'weekly_cases': 0},
                                                                             '6-8': {'weekly_tests_num': 32, 'weekly_newly_tested': 30, 'weekly_cases': 14},
                                                                             '9-11': {'weekly_tests_num': 34, 'weekly_newly_tested': 29, 'weekly_cases': 14}})}),
                               '2020-03-22': defaultdict(None,
                                                         {'לא ידוע': defaultdict(dict,
                                                          {'0-2': {'weekly_tests_num': 14, 'weekly_newly_tested': 14, 'weekly_cases': 0},
                                                           '12-14': {'weekly_tests_num': 0, 'weekly_newly_tested': 0, 'weekly_cases': 0},
                                                           '15-17': {'weekly_tests_num': 0, 'weekly_newly_tested': 0, 'weekly_cases': 0},
                                                           '18-20': {'weekly_tests_num': 0, 'weekly_newly_tested': 0, 'weekly_cases': 0},
                                                           '3-5': {'weekly_tests_num': 14, 'weekly_newly_tested': 14, 'weekly_cases': 0}}),
                               'מחוז אשקלון': defaultdict(dict,
                                                          {'0-2': {'weekly_tests_num': 70, 'weekly_newly_tested': 67, 'weekly_cases': 14},
                                                           '12-14': {'weekly_tests_num': 29, 'weekly_newly_tested': 26, 'weekly_cases': 14},
                                                           '15-17': {'weekly_tests_num': 39, 'weekly_newly_tested': 36, 'weekly_cases': 14},
                                                           '18-20': {'weekly_tests_num': 79, 'weekly_newly_tested': 76, 'weekly_cases': 14},
                                                           '3-5': {'weekly_tests_num': 49, 'weekly_newly_tested': 48, 'weekly_cases': 14},
                                                           '6-8': {'weekly_tests_num': 27, 'weekly_newly_tested': 24, 'weekly_cases': 14}}),
                               'מחוז דרום': defaultdict(dict,
                                                        {'0-2': {'weekly_tests_num': 97, 'weekly_newly_tested': 95, 'weekly_cases': 14},
                                                         '12-14': {'weekly_tests_num': 42, 'weekly_newly_tested': 40, 'weekly_cases': 14},
                                                         '15-17': {'weekly_tests_num': 52, 'weekly_newly_tested': 50, 'weekly_cases': 14},
                                                         '18-20': {'weekly_tests_num': 119, 'weekly_newly_tested': 112, 'weekly_cases': 14},
                                                         '3-5': {'weekly_tests_num': 62, 'weekly_newly_tested': 59, 'weekly_cases': 14}}),
                               'מחוז חיפה': defaultdict(dict,
                                                        {'0-2': {'weekly_tests_num': 86, 'weekly_newly_tested': 77, 'weekly_cases': 14},
                                                         '12-14': {'weekly_tests_num': 20, 'weekly_newly_tested': 19, 'weekly_cases': 14},
                                                         '15-17': {'weekly_tests_num': 20, 'weekly_newly_tested': 19, 'weekly_cases': 14},
                                                         '18-20': {'weekly_tests_num': 125, 'weekly_newly_tested': 115, 'weekly_cases': 14},
                                                         '3-5': {'weekly_tests_num': 50, 'weekly_newly_tested': 47, 'weekly_cases': 14},
                                                         '6-8': {'weekly_tests_num': 25, 'weekly_newly_tested': 23, 'weekly_cases': 0}}),
                               'מחוז ירושלים': defaultdict(dict,
                                                           {'0-2': {'weekly_tests_num': 217, 'weekly_newly_tested': 198, 'weekly_cases': 14},
                                                            '12-14': {'weekly_tests_num': 79, 'weekly_newly_tested': 72, 'weekly_cases': 14},
                                                            '15-17': {'weekly_tests_num': 117, 'weekly_newly_tested': 107, 'weekly_cases': 14},
                                                            '18-20': {'weekly_tests_num': 189, 'weekly_newly_tested': 178, 'weekly_cases': 23},
                                                            '3-5': {'weekly_tests_num': 136, 'weekly_newly_tested': 128, 'weekly_cases': 14},
                                                            '6-8': {'weekly_tests_num': 107, 'weekly_newly_tested': 104, 'weekly_cases': 14}}),
                               'מחוז מרכז': defaultdict(dict,
                                                        {'0-2': {'weekly_tests_num': 296, 'weekly_newly_tested': 286, 'weekly_cases': 14},
                                                         '12-14': {'weekly_tests_num': 163, 'weekly_newly_tested': 155, 'weekly_cases': 16},
                                                         '15-17': {'weekly_tests_num': 222, 'weekly_newly_tested': 204, 'weekly_cases': 21},
                                                         '18-20': {'weekly_tests_num': 491, 'weekly_newly_tested': 451, 'weekly_cases': 37},
                                                         '3-5': {'weekly_tests_num': 227, 'weekly_newly_tested': 213, 'weekly_cases': 14}}),
                               'מחוז צפון': defaultdict(dict,
                                                        {'0-2': {'weekly_tests_num': 126, 'weekly_newly_tested': 115, 'weekly_cases': 14},
                                                         '12-14': {'weekly_tests_num': 43, 'weekly_newly_tested': 42, 'weekly_cases': 14},
                                                         '15-17': {'weekly_tests_num': 62, 'weekly_newly_tested': 56, 'weekly_cases': 14},
                                                         '18-20': {'weekly_tests_num': 188, 'weekly_newly_tested': 182, 'weekly_cases': 14},
                                                         '3-5': {'weekly_tests_num': 70, 'weekly_newly_tested': 67, 'weekly_cases': 14},
                                                         '6-8': {'weekly_tests_num': 56, 'weekly_newly_tested': 52, 'weekly_cases': 14}}),
                               'מחוז תל אביב': defaultdict(dict,
                                                           {'0-2': {'weekly_tests_num': 212, 'weekly_newly_tested': 192, 'weekly_cases': 14},
                                                            '12-14': {'weekly_tests_num': 97, 'weekly_newly_tested': 91, 'weekly_cases': 23},
                                                            '15-17': {'weekly_tests_num': 126, 'weekly_newly_tested': 118, 'weekly_cases': 21},
                                                            '18-20': {'weekly_tests_num': 256, 'weekly_newly_tested': 229, 'weekly_cases': 28},
                                                            '3-5': {'weekly_tests_num': 132, 'weekly_newly_tested': 123, 'weekly_cases': 14}})})})
        # Data Validation
        self._test_four_level_depth_nested_dictionaries(data, results)
