from collections import defaultdict

from covid19_il.tests.data_handler.data_handler_tests_utils import DataHandlerTestsUtils
from covid19_il.data_handler.data_handlers.tested_individuals_scores import TestedIndividualsScores
from covid19_il.data_handler.enums.resource_id import ResourceId


class TestTestedIndividualsScores(DataHandlerTestsUtils):
    """ Tests for Tested Individuals Scores Data Handler Class.

    Methods:
        setUp(self): Announce of starting the class's tests, initialize & verify Age Gender data handler's instance.
        test_get_statistics(self): Tests results data & type of total statistics.
        test_get_statistics_by_date(self): Tests results data & type of statistics by given_first_week_day.

    """

    def setUp(self) -> None:
        """ Announce of starting the class's tests, initialize & verify Tested Individuals Scores data handler's
            instance """
        print("testing Tested Individuals Scores Class...")
        self.data_handler_1 = \
            self._init_mocked_data_handler(json_file_path="json_files/tested_individuals_scores_mocked_data.json",
                                           resource_id_enum=ResourceId.TESTED_INDIVIDUALS_SCORES_RESOURCE_ID)
        self._check_base_step_of_all_methods(data_handler=self.data_handler_1, class_type=TestedIndividualsScores)

    def test_get_statistics(self) -> None:
        """ Tests results data & type of total statistics """
        # Get Data
        data = self.data_handler_1.get_statistics()
        results = defaultdict(None,
                              {'male': defaultdict(int, {'NULL': 6378, 'No': 257010, 'Yes': 54325}),
                               'female': defaultdict(int, {'NULL': 5661, 'No': 288084, 'Yes': 75234}),
                               'NULL': defaultdict(int, {'NULL': 589, 'No': 922, 'Yes': 350})})


        # Data Validation
        self._test_two_level_depth_nested_dictionaries(data, results)

    def test_get_statistics_by_date(self) -> None:
        """ Tests results data & type of statistics by given_first_week_day """
        # Get Data
        data = self.data_handler_1.get_statistics_by_date('2020-10-05')
        results = defaultdict(None,
                              {'NULL':
                                   {'male': 296, 'female': 330, 'NULL': 45},
                               'No': {'male': 17578, 'female': 21223, 'NULL': 130},
                               'Yes': {'male': 4222, 'female': 6725, 'NULL': 8}})

        # Data Validation
        self._test_two_level_depth_nested_dictionaries(data, results)
