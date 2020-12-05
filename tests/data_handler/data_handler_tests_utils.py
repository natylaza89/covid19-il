import unittest
import json
from collections import defaultdict
from numpy import int64 as numpy_int64, float64 as numpy_float64
from typing import Union, DefaultDict, Dict, Any, Generator

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
                                                       Dict[str, Any]) \
                                              -> None:
        """ Tests Dictionary with normal 1 level depth """
        # Check yield type as a generator
        self.assertIsInstance(data, type(_ for _ in range(0)))

        for key, value in data:
            self.assertIsInstance(key, str)
            self.assertIsInstance(value, (int, float, str))

        # Check for values equality
        for data_value, result_value in zip(data, results.values()):
            self.assertDictEqual(data_value, result_value)

    def _test_two_level_depth_nested_dictionaries(self,
                  data: Generator[DefaultDict[str, DefaultDict[str, int]], None, None] or
                        Generator[Dict[str, int], None, None] or
                        Generator[Dict[str, Dict[str, str]], None, None],
                  results: Generator[DefaultDict[str, DefaultDict[str, int]], None, None] or
                           Generator[Dict[str, int], None, None] or
                           Generator[Dict[str, Dict[str, str]], None, None]) \
                  -> None:
        """ Tests Nested Dictionaries with 2 level depth """
        # Check yield type as a generator
        self.assertIsInstance(data, type(_ for _ in range(0)))

        for main_key, main_value in data:
            self.assertIsInstance(main_key, str)
            self.assertIsInstance(main_value, (defaultdict, dict))
            for key, value in main_value.items():
                self.assertIsInstance(key, str)
                self.assertIsInstance(value, (int, float, str, numpy_int64, numpy_float64))
        # Check for values equality
        for data_value, result_value in zip(data, results.values()):
            self.assertDictEqual(data_value, result_value)

    def _test_three_level_depth_nested_dictionaries(self,
                data: Generator[DefaultDict[str, DefaultDict[str, DefaultDict[str, int]]], None, None] or
                      Generator[DefaultDict[str, Dict[str, Dict[str, Union[int, float]]]], None, None] or
                      Generator[DefaultDict[str, DefaultDict[str, Dict[str, Union[int, float, str]]]], None, None],
                results: Generator[DefaultDict[str, DefaultDict[str, DefaultDict[str, int]]], None, None] or
                         Generator[DefaultDict[str, Dict[str, Dict[str, Union[int, float]]]], None, None] or
                         Generator[DefaultDict[str, DefaultDict[str, Dict[str, Union[int, float, str]]]], None, None])\
                -> None:
        """ Tests Nested Dictionaries with 3 level depth """
        # Check yield type as a generator
        self.assertIsInstance(data, type(_ for _ in range(0)))

        for main_key, main_value in data:
            self.assertIsInstance(main_key, str)
            self.assertIsInstance(main_value, (defaultdict, dict))
            for key, value in main_value.items():
                self.assertIsInstance(key, str)
                self.assertIsInstance(value, (defaultdict, dict))
                for sub_key, sub_value in value.items():
                    self.assertIsInstance(sub_key, str)
                    self.assertIsInstance(sub_value, (int, str, numpy_int64, numpy_float64))
        # Check for values equality
        for data_value, result_value in zip(data, results.values()):
            self.assertDictEqual(data_value, result_value)

    def _test_four_level_depth_nested_dictionaries(self,
            data: Generator[DefaultDict[str, DefaultDict[str, DefaultDict[str, Dict[str, int]]]], None, None] or
                  Generator[DefaultDict[str, DefaultDict[str, Dict[str, Dict[str, int or str]]]], None, None],
            results: DefaultDict[str, DefaultDict[str, DefaultDict[str, Dict[str, int]]]] or
                     DefaultDict[str, DefaultDict[str, Dict[str, Dict[str, int or str]]]],) \
            -> None:
        """ Tests Nested Dictionaries with 4 level depth """
        # Check yield type as a generator
        self.assertIsInstance(data, type(_ for _ in range(0)))

        for main_key, main_value in data:
            self.assertIsInstance(main_key, str)
            self.assertIsInstance(main_value, (defaultdict, dict))
            for key, value in main_value.items():
                self.assertIsInstance(key, str)
                self.assertIsInstance(value, (defaultdict, dict))
                for sub_key, sub_value in value.items():
                    self.assertIsInstance(sub_key, str)
                    self.assertIsInstance(sub_value, (defaultdict, dict))
                    for sub2_key, sub2_value in sub_value.items():
                        self.assertIsInstance(sub2_key, str)
                        self.assertIsInstance(sub2_value, (int, str))
        # Check for values equality
        for data_value, result_value in zip(data, results.values()):
            self.assertDictEqual(data_value, result_value)
