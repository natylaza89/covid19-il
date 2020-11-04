from covid19_il.logger.logger import Logger
from covid19_il.data_handler.data_handlers.data_handler import DataHandler
from covid19_il.data_handler.enums.resource_id import ResourceId

from covid19_il.data_handler.data_handlers.area import Area
from covid19_il.data_handler.data_handlers.quarantine import Quarantine
from covid19_il.data_handler.data_handlers.lab_tests import LabTests
from covid19_il.data_handler.data_handlers.tested_individuals import TestedIndividuals
# TODO: resource id 5
from covid19_il.data_handler.data_handlers.recovered import Recovered
from covid19_il.data_handler.data_handlers.hospitalized import Hospitalized
from covid19_il.data_handler.data_handlers.age_gender import AgeGender
from covid19_il.data_handler.data_handlers.medical_staff_morbidity import MedicalStaffMorbidity
from covid19_il.data_handler.data_handlers.deaths import Deaths
from covid19_il.data_handler.data_handlers.young_population import YoungPopulation
from covid19_il.data_handler.data_handlers.cities import Cities


class DataHandlerFactory:
    """ Data Handlers Factory for creating Types of data handlers of different data resources.

    Attributes:
        None

    Methods:
        def get_instance(cls, required_resource_id: ResourceId, json_data: dict = None): get data handler's instance.
        def _create_data_handler(cls, required_resource_id: ResourceId, json_data: dict = None): creates the class
            instance of required data handler.

    """

    data_resources = {}

    @classmethod
    def get_instance(cls, required_resource_id: ResourceId, json_data: dict = None) -> DataHandler or None:
        """ Create or get exist class's instance by memoization which guarantee singleton behaviour.

        Args:
            required_resource_id(ResourceId): enum type of desired data resource id.
            json_data(dict): json data as dictionary for data handler.

        Returns:
            DataHandler or None: data handler's class instance or None object.

        Raises:
            KeyError: concrete error which can occurred when specific instance doesnt exist in dictionary.
        """

        try:
            _ = cls.data_resources[required_resource_id.value]
        except KeyError:
            cls.data_resources[required_resource_id.value] = cls._create_data_handler(required_resource_id, json_data)
        finally:
            return cls.data_resources[required_resource_id.value]

    @classmethod
    def _create_data_handler(cls, required_resource_id: ResourceId, json_data: dict = None) -> DataHandler or None:
        """ Create Required Data Handler for each Data Resource with its unique/special methods.

        Note:
            private method which get called by get_instance's method.

        Args:
            required_resource_id(ResourceId): enum type of desired data resource id.
            json_data(dict): json data as dictionary for data handler.

        Returns:
            DataHandler or None: data handler's class instance or None object.

        Returns:
            DataHandler or None: data handler's class instance or None object.

        """
        switch_case = {
            1: Area(Logger().logger, json_data),
            2: Quarantine(Logger().logger, json_data),
            3: LabTests(Logger().logger, json_data),
            4: TestedIndividuals(Logger().logger, json_data),
            # TODO: 5: TestedIndividualsScores(Logger().logger, json_data),
            6: Recovered(Logger().logger, json_data),
            7: Hospitalized(Logger().logger, json_data),
            8: AgeGender(Logger().logger, json_data),
            9: MedicalStaffMorbidity(Logger().logger, json_data),
            10: Deaths(Logger().logger, json_data),
            11: YoungPopulation(Logger().logger, json_data),
            12: Cities(Logger().logger, json_data),
            "default": None
        }
        return switch_case.get(required_resource_id.value, switch_case['default'])
