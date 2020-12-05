from collections import defaultdict

from tests.data_handler.data_handler_tests_utils import DataHandlerTestsUtils
from covid19_il.data_handler.data_handlers.tested_individuals import TestedIndividuals
from covid19_il.data_handler.enums.resource_id import ResourceId


class TestTestedIndividuals(DataHandlerTestsUtils):
    """ Tests for Tested Individuals Data Handler Class.

    Methods:
        setUp(self): Announce of starting the class's tests, initialize & verify Tested Individuals data handler's
            instance.
        test_tests_results_by_date(self): Validate result's data & types of tests_results_by_date.
        test_amount_of_test_indication(self): Validate result's data & types of amount_of_test_indication.
        test_effects_amount_of_subjects(self): Validate result's data & types of effects_amount_of_subjects.

    """

    def setUp(self) -> None:
        """ Announce of starting the class's tests, initialize & verify Tested Individuals data handler's instance """
        print("testing Tested Individuals Class...")
        self.data_handler_1 = \
            self._init_mocked_data_handler(json_file_path="json_files/tested_individuals_mocked_data.json",
                                           resource_id_enum=ResourceId.TESTED_INDIVIDUALS_RESOURCE_ID)
        self._check_base_step_of_all_methods(data_handler=self.data_handler_1, class_type=TestedIndividuals)

    def test_tests_results_by_date(self) -> None:
        """ Validate result's data & types of tests_results_by_date. """
        # Get Data
        data = self.data_handler_1.tests_results_by_date("2020-10-25")
        results = defaultdict(None,
                              {'חיובי': defaultdict(int, {'זכר': 4, 'נקבה': 1}),
                               'שלילי': defaultdict(int, {'זכר': 49, 'נקבה': 45, 'NULL': 1})})

        # Data Validation
        self._test_two_level_depth_nested_dictionaries(data, results)

    def test_amount_of_test_indication(self) -> None:
        """ Validate result's data & types of amount_of_test_indication.
            amount_of_subjects_ages_60_and_above method has the same logic. """
        # Get Data
        data = self.data_handler_1.amount_of_test_indication()
        results = {'Other': 76, 'Abroad': 13, 'Contact with confirmed': 11}

        # Data Validation
        self._test_one_level_depth_dictionary(data, results)

    def test_effects_amount_of_subjects(self) -> None:
        """ Validate result's data & types of effects_amount_of_subjects. """
        # Get Data
        data = self.data_handler_1.effects_amount_of_subjects()
        results = {'cough': {'False': 74, 'True': 26},
                   'fever': {'False': 90, 'True': 10},
                   'sore_throat': {'False': 99, 'True': 1},
                   'shortness_of_breath': {'False': 98, 'True': 2},
                   'head_ache': {'False': 98, 'True': 2}
                   }

        # Data Validation
        self._test_two_level_depth_nested_dictionaries(data, results)


