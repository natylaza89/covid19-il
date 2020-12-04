from collections import defaultdict
from functools import lru_cache
from typing import Dict, DefaultDict, Generator, Tuple

from covid19_il.logger.logger import Logger
from covid19_il.data_handler.data_handlers.data_handler import DataHandler


class TestedIndividuals(DataHandler):
    """ Covid19_IL Tested Individuals Data Handler.

    Attributes:
        None.

    Methods:
        tests_results_by_date(self, date_string: str): Yields data of the corona result's amount by gender via the data
            frame.
        amount_of_test_indication(self): Yields data of test_indication: amount of test indication's properties via
            the subjects.
        amount_of_subjects_ages_60_and_above(self): Yields data of age group 60+ test status: amount via the subjects.
        effects_amount_of_subjects(self): returns a data holder of top total amount of given column name via data frame.

    """

    def __init__(self, logger: Logger.logger, json_data: Dict) -> None:
        """ Initialize Base Class & Instance Attributes """
        super().__init__(logger, json_data)

    def tests_results_by_date(self, date_string: str) \
            -> Generator[DefaultDict[str, DefaultDict[str, Dict[str, int]]], None, None] or \
               Generator[Tuple[str, str], None, None]:
        """ Yields corona result's amount by gender via the data frame.

        Args:
            date_string(str): desired date as string.

        Yields:
            Tuple[str, DefaultDict[str, Dict[str, int]] or Tuple[str, str]: hospitalized_total_stats's data or
             "No Data" as  bad result.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            df = df[df['test_date'] == date_string]
            df = df[['corona_result', 'gender']]
            ser_group_by = df.groupby('corona_result')['gender'].value_counts()
            data_dict = defaultdict(lambda: defaultdict(int))
            for key, value in ser_group_by.items():
                # key[0] : test result, key[1] : gender
                data_dict[key[0]][key[1]] = value
        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        except IndexError as ie:
            self._logger.exception(ie, "Boolean index did not match indexed array along dimension 0")
        finally:
            if bool(data_dict):
                for item in data_dict.items():
                    yield item
            else:
                yield "No Data", ""

    def _get_value_counts_by_column(self, column_name: str) -> Generator[Dict[str, int], None, None] or \
                                                               Generator[Tuple[str, str], None, None]:
        """ Yields data by column_name(str): value count(int).

        Args:
            column_name(str): column name for value counting.

        Yields:
            Tuple[str, int] or Tuple[str, str]: desired data or "No Data" as bad result.

        """

        data_dict = None
        try:
            data = self._df[column_name].value_counts()
            data_dict = data.to_dict()
        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            if bool(data_dict):
                for item in data_dict.items():
                    yield item
            else:
                yield "No Data", ""

    @lru_cache
    def amount_of_test_indication(self) -> Generator[Dict[str, int], None, None] or \
                                           Generator[Tuple[str, str], None, None]:
        """ Yields data of test_indication: amount of test indication's properties via the subjects.

        Args:
            None.

        Yields:
            Tuple[str, int] or Tuple[str, str]: desired data or "No Data" as bad result.

        """

        return self._get_value_counts_by_column('test_indication')

    @lru_cache
    def amount_of_subjects_ages_60_and_above(self) -> Generator[Dict[str, int], None, None] or \
                                                      Generator[Tuple[str, str], None, None]:
        """ Yields data of age group 60+ test status: amount via the subjects.

        Args:
            None.

        Yields:
            Tuple[str, int] or Tuple[str, str]: desired data or "No Data" as bad result.

        """

        return self._get_value_counts_by_column('age_60_and_above')

    @lru_cache
    def effects_amount_of_subjects(self) -> Generator[Dict[str, Dict[str, int]], None, None] or \
                                            Generator[Tuple[str, str], None, None]:
        """ Yields data of top total amount of symptoms before test date via data frame.

        Args:
            None.

        Yields:
            Tuple[str, Dict[str, int]] or Tuple[str, str]: desired data or "No Data" as bad result.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            df = df[['cough', 'fever', 'sore_throat', 'shortness_of_breath', 'head_ache']]
            data_dict = {}
            for column in df:
                data_dict[column] = {self._string_parser(key): value for key, value
                                     in df[column].value_counts().items()}
        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            if bool(data_dict):
                for item in data_dict.items():
                    yield item
            else:
                yield "No Data", ""

    def _string_parser(self, string: str) -> str:
        """ Returns True/False whether the int value of the string is truthy/falsy.

        Note:
            overridden method - private method which get called by other methods when a string need to be parsed.

        Args:
            string(str): given string for parsing.

        Returns:
            string(str): boolean as string.

        """

        return 'True' if int(string) else 'False'
