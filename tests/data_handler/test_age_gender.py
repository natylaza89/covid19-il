from collections import defaultdict

from tests.data_handler.data_handler_tests_utils import DataHandlerTestsUtils
from covid19_il.data_handler.data_handlers.age_gender import AgeGender
from covid19_il.data_handler.enums.resource_id import ResourceId


class TestAgeGender(DataHandlerTestsUtils):
    """ Tests for Age Gender Data Handler Class.

    Methods:
        setUp(self): Announce of starting the class's tests, initialize & verify Age Gender data handler's instance.
        test_statistics_by_gender(self): Tests results data & type of statistics by gender.
        test_statistics_by_given_first_week_day(self): Tests results data & type of statistics by given_first_week_day.
        test_statistics_by_age_group(self): Tests results data & type of statistics_by_age_group.

    """

    def setUp(self) -> None:
        """ Announce of starting the class's tests, initialize & verify Age Gender data handler's instance """
        print("testing Age Gender Class...")
        self.data_handler_1 = self._init_mocked_data_handler(json_file_path="json_files/age_gender_mocked_data.json",
                                                             resource_id_enum=ResourceId.AGE_GENDER_DATA_RESOURCE_ID)
        self._check_base_step_of_all_methods(data_handler=self.data_handler_1, class_type=AgeGender)

    def test_statistics_by_gender(self) -> None:
        """ Tests results data & type of statistics by gender/ """
        # Get Data
        data = self.data_handler_1.statistics_by_gender()
        results = defaultdict(None,
                              {'זכר': defaultdict(None,
                                  {'2020-03-15':
                                       {'0-19': {'weekly_tests_num': '1088.0', 'weekly_newly_tested': '1037.0', 'weekly_cases': '37.0', 'weekly_deceased': '0.0'},
                                        '20-24': {'weekly_tests_num': '695.0', 'weekly_newly_tested': '650.0', 'weekly_cases': '105.0', 'weekly_deceased': '0.0'},
                                        '25-29': {'weekly_tests_num': '588.0', 'weekly_newly_tested': '563.0', 'weekly_cases': '53.0', 'weekly_deceased': '0.0'},
                                        '30-34': {'weekly_tests_num': '661.0', 'weekly_newly_tested': '608.0', 'weekly_cases': '66.0', 'weekly_deceased': '0.0'},
                                        '35-39': {'weekly_tests_num': '579.0', 'weekly_newly_tested': '528.0', 'weekly_cases': '55.0', 'weekly_deceased': '0.0'},
                                        '40-44': {'weekly_tests_num': '417.0', 'weekly_newly_tested': '384.0', 'weekly_cases': '35.0', 'weekly_deceased': '0.0'},
                                        '45-49': {'weekly_tests_num': '367.0', 'weekly_newly_tested': '328.0', 'weekly_cases': '41.0', 'weekly_deceased': '0.0'},
                                        '50-54': {'weekly_tests_num': '302.0', 'weekly_newly_tested': '273.0', 'weekly_cases': '37.0', 'weekly_deceased': '0.0'},
                                        '55-59': {'weekly_tests_num': '214.0', 'weekly_newly_tested': '187.0', 'weekly_cases': '27.0', 'weekly_deceased': '0.0'},
                                        '60-64': {'weekly_tests_num': '246.0', 'weekly_newly_tested': '222.0', 'weekly_cases': '24.0', 'weekly_deceased': '0.0'},
                                        '65-69': {'weekly_tests_num': '263.0', 'weekly_newly_tested': '230.0', 'weekly_cases': '34.0', 'weekly_deceased': '0.0'},
                                        '70-74': {'weekly_tests_num': '214.0', 'weekly_newly_tested': '179.0', 'weekly_cases': '31.0', 'weekly_deceased': '0.0'},
                                        '75-79': {'weekly_tests_num': '107.0', 'weekly_newly_tested': '96.0', 'weekly_cases': '<15', 'weekly_deceased': '0.0'},
                                        '80+': {'weekly_tests_num': '182.0', 'weekly_newly_tested': '161.0', 'weekly_cases': '<15', 'weekly_deceased': '<15'},
                                        'NULL': {'weekly_tests_num': '292.0', 'weekly_newly_tested': '272.0', 'weekly_cases': '<15', 'weekly_deceased': '0.0'}},
                                   '2020-03-22':
                                       {'0-19': {'weekly_tests_num': '2428.0', 'weekly_newly_tested': '2281.0', 'weekly_cases': '191.0', 'weekly_deceased': '0.0'},
                                        '20-24': {'weekly_tests_num': '2076.0', 'weekly_newly_tested': '1894.0', 'weekly_cases': '219.0', 'weekly_deceased': '0.0'}}}),
                               'לא ידוע': defaultdict(None,
                                {'2020-03-15':
                                     {'0-19': {'weekly_tests_num': '29.0', 'weekly_newly_tested': '29.0', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                      '20-24': {'weekly_tests_num': '<15', 'weekly_newly_tested': '<15', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                      '25-29': {'weekly_tests_num': '<15', 'weekly_newly_tested': '<15', 'weekly_cases': '<15', 'weekly_deceased': '0.0'},
                                      '30-34': {'weekly_tests_num': '<15', 'weekly_newly_tested': '<15', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                      '35-39': {'weekly_tests_num': '<15', 'weekly_newly_tested': '<15', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                      '40-44': {'weekly_tests_num': '<15', 'weekly_newly_tested': '<15', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                      '45-49': {'weekly_tests_num': '<15', 'weekly_newly_tested': '<15', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                      '50-54': {'weekly_tests_num': '<15', 'weekly_newly_tested': '<15', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                      '55-59': {'weekly_tests_num': '<15', 'weekly_newly_tested': '<15', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                      '60-64': {'weekly_tests_num': '0.0', 'weekly_newly_tested': '0.0', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                      '65-69': {'weekly_tests_num': '<15', 'weekly_newly_tested': '<15', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                      '70-74': {'weekly_tests_num': '<15', 'weekly_newly_tested': '<15', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                      '75-79': {'weekly_tests_num': '0.0', 'weekly_newly_tested': '0.0', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                      '80+': {'weekly_tests_num': '0.0', 'weekly_newly_tested': '0.0', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                      'NULL': {'weekly_tests_num': '39.0', 'weekly_newly_tested': '36.0', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'}},
                                 '2020-03-22': {'0-19': {'weekly_tests_num': '92.0', 'weekly_newly_tested': '91.0', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                                '20-24': {'weekly_tests_num': '143.0', 'weekly_newly_tested': '140.0', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'}}}),
                         'נקבה': defaultdict(None,
                                  {'2020-03-15':
                                       {'0-19': {'weekly_tests_num': '899.0', 'weekly_newly_tested': '863.0', 'weekly_cases': '39.0', 'weekly_deceased': '0.0'},
                                        '20-24': {'weekly_tests_num': '623.0', 'weekly_newly_tested': '604.0', 'weekly_cases': '34.0', 'weekly_deceased': '0.0'},
                                        '25-29': {'weekly_tests_num': '749.0', 'weekly_newly_tested': '716.0', 'weekly_cases': '51.0', 'weekly_deceased': '0.0'},
                                        '30-34': {'weekly_tests_num': '700.0', 'weekly_newly_tested': '639.0', 'weekly_cases': '39.0', 'weekly_deceased': '0.0'},
                                        '35-39': {'weekly_tests_num': '521.0', 'weekly_newly_tested': '494.0', 'weekly_cases': '28.0', 'weekly_deceased': '0.0'},
                                        '40-44': {'weekly_tests_num': '474.0', 'weekly_newly_tested': '431.0', 'weekly_cases': '21.0', 'weekly_deceased': '0.0'},
                                        '45-49': {'weekly_tests_num': '384.0', 'weekly_newly_tested': '366.0', 'weekly_cases': '18.0', 'weekly_deceased': '0.0'},
                                        '50-54': {'weekly_tests_num': '387.0', 'weekly_newly_tested': '365.0', 'weekly_cases': '24.0', 'weekly_deceased': '0.0'},
                                        '55-59': {'weekly_tests_num': '350.0', 'weekly_newly_tested': '324.0', 'weekly_cases': '27.0', 'weekly_deceased': '0.0'},
                                        '60-64': {'weekly_tests_num': '305.0', 'weekly_newly_tested': '284.0', 'weekly_cases': '25.0', 'weekly_deceased': '0.0'},
                                        '65-69': {'weekly_tests_num': '222.0', 'weekly_newly_tested': '199.0', 'weekly_cases': '16.0', 'weekly_deceased': '0.0'},
                                        '70-74': {'weekly_tests_num': '164.0', 'weekly_newly_tested': '143.0', 'weekly_cases': '19.0', 'weekly_deceased': '0.0'},
                                        '75-79': {'weekly_tests_num': '83.0', 'weekly_newly_tested': '78.0', 'weekly_cases': '<15', 'weekly_deceased': '0.0'},
                                        '80+': {'weekly_tests_num': '223.0', 'weekly_newly_tested': '188.0', 'weekly_cases': '<15', 'weekly_deceased': '0.0'},
                                        'NULL': {'weekly_tests_num': '310.0', 'weekly_newly_tested': '288.0', 'weekly_cases': '<15', 'weekly_deceased': '0.0'}},
                                   '2020-03-22':
                                       {'0-19': {'weekly_tests_num': '2231.0', 'weekly_newly_tested': '2086.0', 'weekly_cases': '166.0', 'weekly_deceased': '0.0'}}})})

        # Data Validation
        self._test_four_level_depth_nested_dictionaries(data, results)

    def test_statistics_by_given_first_week_day(self) -> None:
        """ Tests results data & type of statistics by given_first_week_day. """
        # Get Data
        data = self.data_handler_1.statistics_by_given_first_week_day('2020-03-15')
        results = defaultdict(None,
                              {'זכר':
                                   {'0-19': {'weekly_tests_num': '1088.0', 'weekly_newly_tested': '1037.0', 'weekly_cases': '37.0', 'weekly_deceased': '0.0'},
                                    '20-24': {'weekly_tests_num': '695.0', 'weekly_newly_tested': '650.0', 'weekly_cases': '105.0', 'weekly_deceased': '0.0'},
                                    '25-29': {'weekly_tests_num': '588.0', 'weekly_newly_tested': '563.0', 'weekly_cases': '53.0', 'weekly_deceased': '0.0'},
                                    '30-34': {'weekly_tests_num': '661.0', 'weekly_newly_tested': '608.0', 'weekly_cases': '66.0', 'weekly_deceased': '0.0'},
                                    '35-39': {'weekly_tests_num': '579.0', 'weekly_newly_tested': '528.0', 'weekly_cases': '55.0', 'weekly_deceased': '0.0'},
                                    '40-44': {'weekly_tests_num': '417.0', 'weekly_newly_tested': '384.0', 'weekly_cases': '35.0', 'weekly_deceased': '0.0'},
                                    '45-49': {'weekly_tests_num': '367.0', 'weekly_newly_tested': '328.0', 'weekly_cases': '41.0', 'weekly_deceased': '0.0'},
                                    '50-54': {'weekly_tests_num': '302.0', 'weekly_newly_tested': '273.0', 'weekly_cases': '37.0', 'weekly_deceased': '0.0'},
                                    '55-59': {'weekly_tests_num': '214.0', 'weekly_newly_tested': '187.0', 'weekly_cases': '27.0', 'weekly_deceased': '0.0'},
                                    '60-64': {'weekly_tests_num': '246.0', 'weekly_newly_tested': '222.0', 'weekly_cases': '24.0', 'weekly_deceased': '0.0'},
                                    '65-69': {'weekly_tests_num': '263.0', 'weekly_newly_tested': '230.0', 'weekly_cases': '34.0', 'weekly_deceased': '0.0'},
                                    '70-74': {'weekly_tests_num': '214.0', 'weekly_newly_tested': '179.0', 'weekly_cases': '31.0', 'weekly_deceased': '0.0'},
                                    '75-79': {'weekly_tests_num': '107.0', 'weekly_newly_tested': '96.0', 'weekly_cases': '<15', 'weekly_deceased': '0.0'},
                                    '80+': {'weekly_tests_num': '182.0', 'weekly_newly_tested': '161.0', 'weekly_cases': '<15', 'weekly_deceased': '<15'},
                                    'NULL': {'weekly_tests_num': '292.0', 'weekly_newly_tested': '272.0', 'weekly_cases': '<15', 'weekly_deceased': '0.0'}},
                               'לא ידוע':
                                   {'0-19': {'weekly_tests_num': '29.0', 'weekly_newly_tested': '29.0', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                    '20-24': {'weekly_tests_num': '<15', 'weekly_newly_tested': '<15', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                    '25-29': {'weekly_tests_num': '<15', 'weekly_newly_tested': '<15', 'weekly_cases': '<15', 'weekly_deceased': '0.0'},
                                    '30-34': {'weekly_tests_num': '<15', 'weekly_newly_tested': '<15', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                    '35-39': {'weekly_tests_num': '<15', 'weekly_newly_tested': '<15', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                    '40-44': {'weekly_tests_num': '<15', 'weekly_newly_tested': '<15', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                    '45-49': {'weekly_tests_num': '<15', 'weekly_newly_tested': '<15', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                    '50-54': {'weekly_tests_num': '<15', 'weekly_newly_tested': '<15', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                    '55-59': {'weekly_tests_num': '<15', 'weekly_newly_tested': '<15', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                    '60-64': {'weekly_tests_num': '0.0', 'weekly_newly_tested': '0.0', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                    '65-69': {'weekly_tests_num': '<15', 'weekly_newly_tested': '<15', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                    '70-74': {'weekly_tests_num': '<15', 'weekly_newly_tested': '<15', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                    '75-79': {'weekly_tests_num': '0.0', 'weekly_newly_tested': '0.0', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                    '80+': {'weekly_tests_num': '0.0', 'weekly_newly_tested': '0.0', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'},
                                    'NULL': {'weekly_tests_num': '39.0', 'weekly_newly_tested': '36.0', 'weekly_cases': '0.0', 'weekly_deceased': '0.0'}},
                               'נקבה':
                                   {'0-19': {'weekly_tests_num': '899.0', 'weekly_newly_tested': '863.0', 'weekly_cases': '39.0', 'weekly_deceased': '0.0'},
                                    '20-24': {'weekly_tests_num': '623.0', 'weekly_newly_tested': '604.0', 'weekly_cases': '34.0', 'weekly_deceased': '0.0'},
                                    '25-29': {'weekly_tests_num': '749.0', 'weekly_newly_tested': '716.0', 'weekly_cases': '51.0', 'weekly_deceased': '0.0'},
                                    '30-34': {'weekly_tests_num': '700.0', 'weekly_newly_tested': '639.0', 'weekly_cases': '39.0', 'weekly_deceased': '0.0'},
                                    '35-39': {'weekly_tests_num': '521.0', 'weekly_newly_tested': '494.0', 'weekly_cases': '28.0', 'weekly_deceased': '0.0'},
                                    '40-44': {'weekly_tests_num': '474.0', 'weekly_newly_tested': '431.0', 'weekly_cases': '21.0', 'weekly_deceased': '0.0'},
                                    '45-49': {'weekly_tests_num': '384.0', 'weekly_newly_tested': '366.0', 'weekly_cases': '18.0', 'weekly_deceased': '0.0'},
                                    '50-54': {'weekly_tests_num': '387.0', 'weekly_newly_tested': '365.0', 'weekly_cases': '24.0', 'weekly_deceased': '0.0'},
                                    '55-59': {'weekly_tests_num': '350.0', 'weekly_newly_tested': '324.0', 'weekly_cases': '27.0', 'weekly_deceased': '0.0'},
                                    '60-64': {'weekly_tests_num': '305.0', 'weekly_newly_tested': '284.0', 'weekly_cases': '25.0', 'weekly_deceased': '0.0'},
                                    '65-69': {'weekly_tests_num': '222.0', 'weekly_newly_tested': '199.0', 'weekly_cases': '16.0', 'weekly_deceased': '0.0'},
                                    '70-74': {'weekly_tests_num': '164.0', 'weekly_newly_tested': '143.0', 'weekly_cases': '19.0', 'weekly_deceased': '0.0'},
                                    '75-79': {'weekly_tests_num': '83.0', 'weekly_newly_tested': '78.0', 'weekly_cases': '<15', 'weekly_deceased': '0.0'},
                                    '80+': {'weekly_tests_num': '223.0', 'weekly_newly_tested': '188.0', 'weekly_cases': '<15', 'weekly_deceased': '0.0'},
                                    'NULL': {'weekly_tests_num': '310.0', 'weekly_newly_tested': '288.0', 'weekly_cases': '<15', 'weekly_deceased': '0.0'}}})

        # Data Validation
        self._test_three_level_depth_nested_dictionaries(data, results)

    def test_statistics_by_age_group(self) -> None:
        """ Tests results data & type of statistics_by_age_group """
        # Get Data
        data = self.data_handler_1.statistics_by_age_group()
        results = defaultdict(None,
                              {'0-19': {'weekly_tests_num': {'min': 29, 'max': 2428, 'mean': 1127.8333333333333, 'total': 6767},
                                        'weekly_newly_tested': {'min': 29, 'max': 2281, 'mean': 1064.5, 'total': 6387},
                                        'weekly_cases': {'min': 0, 'max': 191, 'mean': 72.16666666666667, 'total': 433},
                                        'weekly_deceased': {'min': 0, 'max': 0, 'mean': 0.0, 'total': 0}},
                               '20-24': {'weekly_tests_num': {'min': 14, 'max': 2076, 'mean': 710.2, 'total': 3551},
                                         'weekly_newly_tested': {'min': 14, 'max': 1894, 'mean': 660.4, 'total': 3302},
                                         'weekly_cases': {'min': 0, 'max': 219, 'mean': 71.6, 'total': 358},
                                         'weekly_deceased': {'min': 0, 'max': 0, 'mean': 0.0, 'total': 0}},
                               '25-29': {'weekly_tests_num': {'min': 14, 'max': 749, 'mean': 450.3333333333333, 'total': 1351},
                                         'weekly_newly_tested': {'min': 14, 'max': 716, 'mean': 431.0, 'total': 1293},
                                         'weekly_cases': {'min': 14, 'max': 53, 'mean': 39.333333333333336, 'total': 118},
                                         'weekly_deceased': {'min': 0, 'max': 0, 'mean': 0.0, 'total': 0}},
                               '30-34': {'weekly_tests_num': {'min': 14, 'max': 700, 'mean': 458.3333333333333, 'total': 1375},
                                         'weekly_newly_tested': {'min': 14, 'max': 639, 'mean': 420.3333333333333, 'total': 1261},
                                         'weekly_cases': {'min': 0, 'max': 66, 'mean': 35.0, 'total': 105},
                                         'weekly_deceased': {'min': 0, 'max': 0, 'mean': 0.0, 'total': 0}},
                               '35-39': {'weekly_tests_num': {'min': 14, 'max': 579, 'mean': 371.3333333333333, 'total': 1114},
                                         'weekly_newly_tested': {'min': 14, 'max': 528, 'mean': 345.3333333333333, 'total': 1036},
                                         'weekly_cases': {'min': 0, 'max': 55, 'mean': 27.666666666666668, 'total': 83},
                                         'weekly_deceased': {'min': 0, 'max': 0, 'mean': 0.0, 'total': 0}},
                               '40-44': {'weekly_tests_num': {'min': 14, 'max': 474, 'mean': 301.6666666666667, 'total': 905},
                                         'weekly_newly_tested': {'min': 14, 'max': 431, 'mean': 276.3333333333333, 'total': 829},
                                         'weekly_cases': {'min': 0, 'max': 35, 'mean': 18.666666666666668, 'total': 56},
                                         'weekly_deceased': {'min': 0, 'max': 0, 'mean': 0.0, 'total': 0}},
                               '45-49': {'weekly_tests_num': {'min': 14, 'max': 384, 'mean': 255.0, 'total': 765},
                                         'weekly_newly_tested': {'min': 14, 'max': 366, 'mean': 236.0, 'total': 708},
                                         'weekly_cases': {'min': 0, 'max': 41, 'mean': 19.666666666666668, 'total': 59},
                                         'weekly_deceased': {'min': 0, 'max': 0, 'mean': 0.0, 'total': 0}},
                               '50-54': {'weekly_tests_num': {'min': 14, 'max': 387, 'mean': 234.33333333333334, 'total': 703},
                                         'weekly_newly_tested': {'min': 14, 'max': 365, 'mean': 217.33333333333334, 'total': 652},
                                         'weekly_cases': {'min': 0, 'max': 37, 'mean': 20.333333333333332, 'total': 61},
                                         'weekly_deceased': {'min': 0, 'max': 0, 'mean': 0.0, 'total': 0}},
                               '55-59': {'weekly_tests_num': {'min': 14, 'max': 350, 'mean': 192.66666666666666, 'total': 578},
                                         'weekly_newly_tested': {'min': 14, 'max': 324, 'mean': 175.0, 'total': 525},
                                         'weekly_cases': {'min': 0, 'max': 27, 'mean': 18.0, 'total': 54},
                                         'weekly_deceased': {'min': 0, 'max': 0, 'mean': 0.0, 'total': 0}},
                               '60-64': {'weekly_tests_num': {'min': 0, 'max': 305, 'mean': 183.66666666666666, 'total': 551},
                                         'weekly_newly_tested': {'min': 0, 'max': 284, 'mean': 168.66666666666666, 'total': 506},
                                         'weekly_cases': {'min': 0, 'max': 25, 'mean': 16.333333333333332, 'total': 49},
                                         'weekly_deceased': {'min': 0, 'max': 0, 'mean': 0.0, 'total': 0}},
                               '65-69': {'weekly_tests_num': {'min': 14, 'max': 263, 'mean': 166.33333333333334, 'total': 499},
                                         'weekly_newly_tested': {'min': 14, 'max': 230, 'mean': 147.66666666666666, 'total': 443},
                                         'weekly_cases': {'min': 0, 'max': 34, 'mean': 16.666666666666668, 'total': 50},
                                         'weekly_deceased': {'min': 0, 'max': 0, 'mean': 0.0, 'total': 0}},
                               '70-74': {'weekly_tests_num': {'min': 14, 'max': 214, 'mean': 130.66666666666666, 'total': 392},
                                         'weekly_newly_tested': {'min': 14, 'max': 179, 'mean': 112.0, 'total': 336},
                                         'weekly_cases': {'min': 0, 'max': 31, 'mean': 16.666666666666668, 'total': 50},
                                         'weekly_deceased': {'min': 0, 'max': 0, 'mean': 0.0, 'total': 0}},
                               '75-79': {'weekly_tests_num': {'min': 0, 'max': 107, 'mean': 63.333333333333336, 'total': 190},
                                         'weekly_newly_tested': {'min': 0, 'max': 96, 'mean': 58.0, 'total': 174},
                                         'weekly_cases': {'min': 0, 'max': 14, 'mean': 9.333333333333334, 'total': 28},
                                         'weekly_deceased': {'min': 0, 'max': 0, 'mean': 0.0, 'total': 0}},
                               '80+': {'weekly_tests_num': {'min': 0, 'max': 223, 'mean': 135.0, 'total': 405},
                                       'weekly_newly_tested': {'min': 0, 'max': 188, 'mean': 116.33333333333333, 'total': 349},
                                       'weekly_cases': {'min': 0, 'max': 14, 'mean': 9.333333333333334, 'total': 28},
                                       'weekly_deceased': {'min': 0, 'max': 14, 'mean': 4.666666666666667, 'total': 14}},
                               'NULL': {'weekly_tests_num': {'min': 39, 'max': 310, 'mean': 213.66666666666666, 'total': 641},
                                        'weekly_newly_tested': {'min': 36, 'max': 288, 'mean': 198.66666666666666, 'total': 596},
                                        'weekly_cases': {'min': 0, 'max': 14, 'mean': 9.333333333333334, 'total': 28},
                                        'weekly_deceased': {'min': 0, 'max': 0, 'mean': 0.0, 'total': 0}}})
        # Data Validation
        self._test_three_level_depth_nested_dictionaries(data, results)
