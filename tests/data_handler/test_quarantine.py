from covid19_il.tests.data_handler.data_handler_tests_utils import DataHandlerTestsUtils
from covid19_il.data_handler.data_handlers.quarantine import Quarantine
from covid19_il.data_handler.enums.resource_id import ResourceId


class TestQuarantine(DataHandlerTestsUtils):
    """ Tests for MQuarantine Data Handler Class.

    Methods:
        setUp(self): Announce of starting the class's tests, initialize & verify Medical Staff Morbidity data handler's
            instance.
        test_isolated_today_contact_with_confirmed(self): Validate result's data & types of
            isolated_today_contact_with_confirmed. isolated_today_abroad, new_contact_with_confirmed and
            new_from_abroad methods have the same logic.
    """

    def setUp(self) -> None:
        """ Announce of starting the class's tests, initialize & verify Quarantine data handler's instance """
        print("testing Quarantine Class...")
        self.data_handler_1 = \
            self._init_mocked_data_handler(json_file_path="json_files/quarantine_mocked_data.json",
                                           resource_id_enum=ResourceId.QUARANTINE_RESOURCE_ID)
        self._check_base_step_of_all_methods(data_handler=self.data_handler_1, class_type=Quarantine)

    def test_isolated_today_contact_with_confirmed(self) -> None:
        """ Validate result's data & types of isolated_today_contact_with_confirmed.
            isolated_today_abroad, new_contact_with_confirmed and new_from_abroad methods have the same logic. """
        # Get Data
        data = self.data_handler_1.isolated_today_contact_with_confirmed()
        results = {'2020-10-24': '38074',
                   '2020-10-23': '40270',
                   '2020-10-22': '43707',
                   '2020-10-21': '46275',
                   '2020-10-20': '49581',
                   '2020-10-19': '52726',
                   '2020-10-18': '56041',
                   '2020-10-17': '57557',
                   '2020-10-16': '62220',
                   '2020-10-15': '70072',
                   '2020-10-14': '75247',
                   '2020-10-13': '78829',
                   '2020-10-12': '78015',
                   '2020-10-11': '77198',
                   '2020-10-10': '76306',
                   '2020-10-09': '79632',
                   '2020-10-08': '85333',
                   '2020-10-07': '86487',
                   '2020-10-06': '86803',
                   '2020-10-05': '85754'}
        # Data Validation
        self._test_one_level_depth_dictionary(data, results)
