from types import GeneratorType

from tests.data_handler.data_handler_tests_utils import DataHandlerTestsUtils
from covid19_il.data_handler.data_handlers.lab_tests import LabTests
from covid19_il.data_handler.enums.resource_id import ResourceId


class TestLabTests(DataHandlerTestsUtils):
    """ Tests for Lab Tests Handler Class.

    Methods:
        setUp(self): Announce of starting the class's tests, initialize & verify cities data handler's instance.
        test_corona_results(self): Tests results data & type of corona results.
            same logic as: lab_tests_statistics, is_first_test_statistics, test_for_corona_statistics.
        test_tests_results_data_by_test_date(self): Tests results data & type of tests_results_data_by_test_date.

    """

    def setUp(self) -> None:
        """ Announce of starting the class's tests, initialize & verify Lab Tests data handler's instance """
        print("testing Lab Tests Class...")
        self.data_handler_1 = self._init_mocked_data_handler(json_file_path="json_files/lab_tests_mocked_data.json",
                                                             resource_id_enum=ResourceId.LAB_TESTS_RESOURCE_ID)
        self._check_base_step_of_all_methods(data_handler=self.data_handler_1, class_type=LabTests)

    def test_corona_results(self) -> None:
        """ Tests results data & type of corona results.
            same logic as: lab_tests_statistics, is_first_test_statistics, test_for_corona_statistics. """
        # Get Data
        data = self.data_handler_1.corona_results()
        results = {'שלילי': 1409, 'חיובי': 117, 'בעבודה': 20, 'לא בוצע/פסול 999': 3, 'לא ודאי ישן': 1}
        # Data Validation
        self._test_one_level_depth_dictionary(data, results)

    def test_tests_results_data_by_test_date(self) -> None:
        """ Tests results data & type of tests_results_data_by_test_date. """
        # Get Data
        data = self.data_handler_1.tests_results_data_by_test_date("2020-03-11")
        results = (LabTests.test(corona_result='בעבודה', lab_id='3', test_for_corona_diagnosis='1', is_first_Test='Yes'),
                   LabTests.test(corona_result='חיובי', lab_id='1', test_for_corona_diagnosis='1', is_first_Test='Yes'),
                   LabTests.test(corona_result='חיובי', lab_id='32', test_for_corona_diagnosis='0', is_first_Test='No'),
                   LabTests.test(corona_result='חיובי', lab_id='32', test_for_corona_diagnosis='1', is_first_Test='No'),
                   LabTests.test(corona_result='חיובי', lab_id='32', test_for_corona_diagnosis='1', is_first_Test='Yes'),
                   LabTests.test(corona_result='לא ודאי ישן', lab_id='3', test_for_corona_diagnosis='1', is_first_Test='Yes'),
                   LabTests.test(corona_result='שלילי', lab_id='3', test_for_corona_diagnosis='1', is_first_Test='No'),
                   LabTests.test(corona_result='שלילי', lab_id='3', test_for_corona_diagnosis='1', is_first_Test='Yes'),
                   LabTests.test(corona_result='שלילי', lab_id='32', test_for_corona_diagnosis='0', is_first_Test='No'),
                   LabTests.test(corona_result='שלילי', lab_id='32', test_for_corona_diagnosis='1', is_first_Test='No'),
                   LabTests.test(corona_result='שלילי', lab_id='32', test_for_corona_diagnosis='1', is_first_Test='Yes'),
                   LabTests.test(corona_result='שלילי', lab_id='4', test_for_corona_diagnosis='1', is_first_Test='Yes'),
                   LabTests.test(corona_result='חיובי', lab_id='1', test_for_corona_diagnosis='0', is_first_Test='No'),
                   LabTests.test(corona_result='חיובי', lab_id='1', test_for_corona_diagnosis='1', is_first_Test='No'),
                   LabTests.test(corona_result='חיובי', lab_id='2', test_for_corona_diagnosis='0', is_first_Test='No'),
                   LabTests.test(corona_result='חיובי', lab_id='32', test_for_corona_diagnosis='1', is_first_Test='Yes'),
                   LabTests.test(corona_result='שלילי', lab_id='2', test_for_corona_diagnosis='0', is_first_Test='No'),
                   LabTests.test(corona_result='שלילי', lab_id='2', test_for_corona_diagnosis='1', is_first_Test='No'),
                   LabTests.test(corona_result='שלילי', lab_id='2', test_for_corona_diagnosis='1', is_first_Test='Yes'),
                   LabTests.test(corona_result='שלילי', lab_id='32', test_for_corona_diagnosis='1', is_first_Test='Yes'),
                   LabTests.test(corona_result='חיובי', lab_id='4', test_for_corona_diagnosis='0', is_first_Test='No'), 
                   LabTests.test(corona_result='חיובי', lab_id='4', test_for_corona_diagnosis='1', is_first_Test='No'),
                   LabTests.test(corona_result='חיובי', lab_id='4', test_for_corona_diagnosis='1', is_first_Test='Yes'),
                   LabTests.test(corona_result='חיובי', lab_id='17', test_for_corona_diagnosis='0', is_first_Test='No'), 
                   LabTests.test(corona_result='בעבודה', lab_id='6', test_for_corona_diagnosis='0', is_first_Test='No'), 
                   LabTests.test(corona_result='בעבודה', lab_id='6', test_for_corona_diagnosis='1', is_first_Test='No'),
                   LabTests.test(corona_result='בעבודה', lab_id='6', test_for_corona_diagnosis='1', is_first_Test='Yes'),
                   LabTests.test(corona_result='שלילי', lab_id='32', test_for_corona_diagnosis='1', is_first_Test='Yes'),
                   LabTests.test(corona_result='שלילי', lab_id='17', test_for_corona_diagnosis='1', is_first_Test='No'), 
                   LabTests.test(corona_result='שלילי', lab_id='5', test_for_corona_diagnosis='0', is_first_Test='No'), 
                   LabTests.test(corona_result='שלילי', lab_id='5', test_for_corona_diagnosis='0', is_first_Test='No'))

        # Data Validation
        self.assertIsInstance(data, GeneratorType)
        for data_item, results_item in zip(data, results):
            self.assertIsInstance(data_item, LabTests.test)
            self.assertTupleEqual(data_item, results_item)
