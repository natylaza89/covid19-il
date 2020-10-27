from collections import defaultdict

from covid19_il.tests.data_handler.data_handler_tests_utils import DataHandlerTestsUtils
from covid19_il.data_handler.data_handlers.medical_staff_morbidity import MedicalStaffMorbidity
from covid19_il.data_handler.enums.resource_id import ResourceId


class TestMedicalStaffMorbidity(DataHandlerTestsUtils):
    """ Tests for Medical Staff Morbidity Data Handler Class.

    Methods:
        setUp(self): Announce of starting the class's tests, initialize & verify Medical Staff Morbidity data handler's
            instance.
        test_confirmed_cases(self): Validate result's data & types of confirmed_cases. isolated_cases method has same
            logic.
        test_isolated_cases_by_date(self): Validate result's data & types of test_confirmed_cases_by_date.
            isolated_cases_by_date method has same logic.
        test_confirmed_cases_statistics(self): Validate result's data & types of test_confirmed_cases_statistics.
            isolated_cases_statistics method has same logic.
    """

    def setUp(self) -> None:
        """ Announce of starting the class's tests, initialize & verify deaths data handler's instance """
        print("testing Medical Staff Morbidity Class...")
        self.data_handler_1 = \
            self._init_mocked_data_handler(json_file_path="json_files/medical_staff_morbidity_mocked_data.json",
                                           resource_id_enum=ResourceId.MEDICAL_STAFF_MORBIDITY_RESOURCE_ID)
        self._check_base_step_of_all_methods(data_handler=self.data_handler_1, class_type=MedicalStaffMorbidity)

    def test_confirmed_cases(self) -> None:
        """ Validate result's data & types of confirmed_cases.
            isolated_cases method has same logic. """
        # Get Data
        data = self.data_handler_1.confirmed_cases()
        results = {'2020-03-15': {'confirmed_cases_physicians': '<15', 'confirmed_cases_nurses': '<15', 'confirmed_cases_other_healthcare_workers': '<15'},
                   '2020-03-16': {'confirmed_cases_physicians': '<15', 'confirmed_cases_nurses': '<15', 'confirmed_cases_other_healthcare_workers': '<15'},
                   '2020-03-17': {'confirmed_cases_physicians': '<15', 'confirmed_cases_nurses': '<15', 'confirmed_cases_other_healthcare_workers': '<15'},
                   '2020-03-18': {'confirmed_cases_physicians': '<15', 'confirmed_cases_nurses': '<15', 'confirmed_cases_other_healthcare_workers': '<15'},
                   '2020-03-19': {'confirmed_cases_physicians': '<15', 'confirmed_cases_nurses': '<15', 'confirmed_cases_other_healthcare_workers': '<15'},
                   '2020-03-21': {'confirmed_cases_physicians': '<15', 'confirmed_cases_nurses': '<15', 'confirmed_cases_other_healthcare_workers': '<15'},
                   '2020-03-22': {'confirmed_cases_physicians': '<15', 'confirmed_cases_nurses': '<15', 'confirmed_cases_other_healthcare_workers': '<15'},
                   '2020-03-23': {'confirmed_cases_physicians': '<15', 'confirmed_cases_nurses': '18', 'confirmed_cases_other_healthcare_workers': '<15'},
                   '2020-03-24': {'confirmed_cases_physicians': '15', 'confirmed_cases_nurses': '18', 'confirmed_cases_other_healthcare_workers': '<15'},
                   '2020-03-25': {'confirmed_cases_physicians': '17', 'confirmed_cases_nurses': '20', 'confirmed_cases_other_healthcare_workers': '15'},
                   '2020-03-26': {'confirmed_cases_physicians': '18', 'confirmed_cases_nurses': '31', 'confirmed_cases_other_healthcare_workers': '19'},
                   '2020-03-27': {'confirmed_cases_physicians': '19', 'confirmed_cases_nurses': '33', 'confirmed_cases_other_healthcare_workers': '29'},
                   '2020-03-28': {'confirmed_cases_physicians': '31', 'confirmed_cases_nurses': '34', 'confirmed_cases_other_healthcare_workers': '39'},
                   '2020-03-29': {'confirmed_cases_physicians': '22', 'confirmed_cases_nurses': '37', 'confirmed_cases_other_healthcare_workers': '40'},
                   '2020-03-30': {'confirmed_cases_physicians': '22', 'confirmed_cases_nurses': '47', 'confirmed_cases_other_healthcare_workers': '45'},
                   '2020-03-31': {'confirmed_cases_physicians': '24', 'confirmed_cases_nurses': '51', 'confirmed_cases_other_healthcare_workers': '48'},
                   '2020-04-01': {'confirmed_cases_physicians': '24', 'confirmed_cases_nurses': '71', 'confirmed_cases_other_healthcare_workers': '53'},
                   '2020-04-02': {'confirmed_cases_physicians': '28', 'confirmed_cases_nurses': '79', 'confirmed_cases_other_healthcare_workers': '61'},
                   '2020-04-03': {'confirmed_cases_physicians': '26', 'confirmed_cases_nurses': '69', 'confirmed_cases_other_healthcare_workers': '71'},
                   '2020-04-05': {'confirmed_cases_physicians': '30', 'confirmed_cases_nurses': '87', 'confirmed_cases_other_healthcare_workers': '66'}}
        # Data Validation
        self._test_two_level_depth_nested_dictionaries(data, results)

    def test_isolated_cases_by_date(self) -> None:
        """ Validate result's data & types of test_confirmed_cases_by_date.
            isolated_cases_by_date method has same logic. """
        # Get Data
        data = self.data_handler_1.isolated_cases_by_date("2020-03-17")
        results = {'isolated_physicians': '503', 'isolated_nurses': '623', 'isolated_other_healthcare_workers': '602'}
        # Data Validation
        self._test_one_level_depth_dictionary(data, results)

    def test_confirmed_cases_statistics(self) -> None:
        """ Validate result's data & types of test_confirmed_cases_statistics.
            isolated_cases_statistics method has same logic. """
        # Get Data
        data = self.data_handler_1.confirmed_cases_statistics()
        results = {'confirmed_cases_physicians':
                       {'min': 14, 'max': 31, 'mean': 19.4, 'sum': 388},
                   'confirmed_cases_nurses':
                       {'min': 14, 'max': 87, 'mean': 34.65, 'sum': 693},
                   'confirmed_cases_other_healthcare_workers':
                       {'min': 14, 'max': 71, 'mean': 30.6, 'sum': 612}}
        # Data Validation
        self._test_two_level_depth_nested_dictionaries(data, results)
