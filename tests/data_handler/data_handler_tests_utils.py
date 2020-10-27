import unittest
import json
from collections import defaultdict
from numpy import int64 as numpy_int64, float64 as numpy_float64
from typing import Union, DefaultDict, Dict, Any

from covid19_il.data_handler.data_handlers_factory.data_handler_factory import DataHandlerFactory
from covid19_il.data_handler.data_handlers.data_handler import DataHandler
from covid19_il.data_handler.enums.resource_id import ResourceId


class DataHandlerTestsUtils(unittest.TestCase):

    def tearDown(self) -> None:
        """ Announce of finishing the class's tests """
        print(f"finished testing {self.__class__.__name__[4:]} Class...")

    def _init_mocked_data_handler(self, json_file_path: str, resource_id_enum: ResourceId) -> type.__class__:
        """ Get mocked data from json file , initialize data handler's instance and return it """
        with open(json_file_path) as json_file:
            mocked_json_data = json.load(json_file)
            return DataHandlerFactory.get_instance(resource_id_enum, mocked_json_data)

    def _check_base_step_of_all_methods(self, data_handler: DataHandler, class_type: Union) -> None:
        """ General base test for all methods """
        # Check instance creation
        self.assertIsInstance(data_handler, class_type)
        # Check unequally of different memory addresses of copied df
        data_dict = data_handler._get_clean_copy_df_data()
        self.assertNotEqual(id(data_dict), id(data_handler.df))

    def _test_one_level_depth_dictionary(self,
                                              data: DefaultDict[str, Any] or
                                                    Dict[str, Any],
                                              results: DefaultDict[str, Any] or
                                                       Dict[str, Any]) -> None:
        """ Tests Dictionary with normal 1 level depth """
        self.assertIsInstance(data, (dict, defaultdict))
        self.assertDictEqual(data, results)
        for key, value in data.items():
            self.assertIsInstance(key, str)
            self.assertIsInstance(value, (int, float, str))

    def _test_two_level_depth_nested_dictionaries(self,
                                                  data: DefaultDict[str, DefaultDict[str, int]] or
                                                        Dict[str, Dict[str, str]],
                                                  results: DefaultDict[str, DefaultDict[str, int] or
                                                           Dict[str, int]] or
                                                           Dict[str, Dict[str, str]]) -> None:
        """ Tests Nested Dictionaries with 2 level depth """
        # check returned type
        self.assertIsInstance(data, (defaultdict, dict))
        for item in data.values():
            self.assertIsInstance(item, (defaultdict, dict))
            for key, value in item.items():
                self.assertIsInstance(key, str)
                self.assertIsInstance(value, (int, float, str, numpy_int64, numpy_float64))
        # check for values equality
        for data_value, result_value in zip(data.values(), results.values()):
            self.assertDictEqual(data_value, result_value)

    def _test_three_level_depth_nested_dictionaries(self,
                                                    data: DefaultDict[str, DefaultDict[str, DefaultDict[str, int]]],
                                                    results: DefaultDict[str, DefaultDict[str, DefaultDict[str, int]]]) \
            -> None:
        """ Tests Nested Dictionaries with 3 level depth """
        # Check returned type
        self.assertIsInstance(data, defaultdict)
        for item in data.values():
            self.assertIsInstance(item, defaultdict)
            for key, value in item.items():
                self.assertIsInstance(key, str)
                self.assertIsInstance(value, defaultdict)
                for sub_key, sub_value in value.items():
                    self.assertIsInstance(sub_key, str)
                    self.assertIsInstance(sub_value, int)
        # check for values equality
        for data_value, result_value in zip(data.values(), results.values()):
            self.assertDictEqual(data_value, result_value)
