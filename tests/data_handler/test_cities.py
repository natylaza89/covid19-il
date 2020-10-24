import unittest
import json
from collections import defaultdict
from numpy import int64 as numpy_int64, float64 as numpy_float64

from covid19_il.data_handler.data_handlers_factory.data_handler_factory import DataHandlerFactory
from covid19_il.data_handler.data_handlers.cities import Cities
from covid19_il.data_handler.enums.resource_id import ResourceId


class TestCities(unittest.TestCase):
    """ Data Handler Factory for creating Types of resource data handlers.

    Methods:
        def setUp(self): Announce of starting the class's tests, initialize & verify cities data handler's instance.
        def tearDown(self): announce of finishing the class's tests

    """

    def setUp(self) -> None:
        """ Announce of starting the class's tests, initialize & verify Cities data handler's instance """
        print("testing Cities Class...")
        with open("json_files/cities_mocked_data.json") as json_file:
            mocked_json_data = json.load(json_file)
            self.data_handler_1 = DataHandlerFactory.get_instance(ResourceId.CITIES_POPULATION_RESOURCE_ID,
                                                                  mocked_json_data)
        self._check_base_step_of_all_methods()

    def tearDown(self) -> None:
        """ Announce of finishing the class's tests """
        print("finished testing Cities Class...")

    def _check_base_step_of_all_methods(self) -> None:
        # Check instance creation
        self.assertIsInstance(self.data_handler_1, Cities)
        # Check unequally of different memory addresses of copied df
        data_dict = self.data_handler_1._get_clean_copy_df_data()
        self.assertNotEqual(id(data_dict), id(self.data_handler_1.df))

    def test_cities_by_date(self) -> None:
        """ Tests results of tests cities by specific date and its results as city's tuples """
        # get data from method
        data = self.data_handler_1.cities_by_date("2020-10-03")
        results = defaultdict(None, {"אבו ג'ווייעד (שבט)": Cities.city(City_name="אבו ג'ווייעד (שבט)", City_code='967', Date='2020-10-03', Cumulative_verified_cases='0', Cumulated_recovered='0', Cumulated_deaths='0', Cumulated_number_of_tests='225', Cumulated_number_of_diagnostic_tests='225'),
                                     'אבו גוש': Cities.city(City_name='אבו גוש', City_code='472', Date='2020-10-03', Cumulative_verified_cases='206', Cumulated_recovered='178', Cumulated_deaths='0', Cumulated_number_of_tests='4101', Cumulated_number_of_diagnostic_tests='3993')})
        # check returned type
        self.assertIs(type(data), defaultdict)
        for item in data.values():
            self.assertIs(type(item), Cities.city)
        # check for values equality
        for data_value, result_value in zip(data.values(), results.values()):
            self.assertTupleEqual(data_value, result_value)

    def test_top_cases_in_cities(self) -> None:
        """ Tests results data & type of top cases in cities  """
        # Get Data
        results = defaultdict(None,
                              {'Cumulative_verified_cases': defaultdict(int, {'אבו גוש': 211, "אבו ג'ווייעד (שבט)": 14}),
                               'Cumulated_recovered': defaultdict(int, {'אבו גוש': 206, "אבו ג'ווייעד (שבט)": 0}),
                               'Cumulated_deaths': defaultdict(int, {"אבו ג'ווייעד (שבט)": 0, 'אבו גוש': 0}),
                               'Cumulated_number_of_tests': defaultdict(int, {'אבו גוש': 4508, "אבו ג'ווייעד (שבט)": 250}),
                               'Cumulated_number_of_diagnostic_tests': defaultdict(int, {'אבו גוש': 4365, "אבו ג'ווייעד (שבט)": 250})
                               })
        data = self.data_handler_1.top_cases_in_cities()
        # Check returned type
        self.assertIs(type(data), defaultdict)
        for item in data.values():
            self.assertIs(type(item), defaultdict)
            for key, value in item.items():
                self.assertIsInstance(key, str)
                self.assertIsInstance(value, int)
        # check for values equality
        for data_value, result_value in zip(data.values(), results.values()):
            self.assertDictEqual(data_value, result_value)

    def test_cases_statistics(self) -> None:
        """ Tests the test cases statistics data & type """
        # Get Data
        results = {'Cumulative_verified_cases': {'min': 0, 'max': 212, 'mean': 25.96, 'sum': 12980},
                   'Cumulated_recovered': {'min': 0, 'max': 206, 'mean': 18.502, 'sum': 9251},
                   'Cumulated_deaths': {'min': 0, 'max': 0, 'mean': 0.0, 'sum': 0},
                   'Cumulated_number_of_tests': {'min': 0, 'max': 4584, 'mean': 677.404, 'sum': 338702},
                   'Cumulated_number_of_diagnostic_tests': {'min': 0, 'max': 4439, 'mean': 665.46, 'sum': 332730}}
        data = self.data_handler_1.cases_statistics()
        # Check returned type
        self.assertIs(type(data), dict)
        for item in data.values():
            self.assertIs(type(item), dict)
            for key, value in item.items():
                self.assertIsInstance(key, str)
                self.assertTrue((type(value) is numpy_int64) or (type(value) is numpy_float64))
        # check for values equality
        for data_value, result_value in zip(data.values(), results.values()):
            self.assertDictEqual(data_value, result_value)
