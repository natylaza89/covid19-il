from collections import defaultdict
from functools import lru_cache
from typing import Dict, DefaultDict

from covid19_il.logger.logger import Logger
from covid19_il.data_handler.data_handlers.data_handler import DataHandler


class TestedIndividuals(DataHandler):
    """ Covid19_IL Tested Individuals Data Handler.

    Attributes:
        None.

    Methods:
        tests_results_by_date(self, date_string: str): returns data holder of the corona result's amount by gender via
            the data frame.
        amount_of_test_indication(self): returns a dictionary of test_indication: amount of test indication's properties
            via the subjects.
        amount_of_subjects_ages_60_and_above(self): returns a dictionary of age group 60+ test status: amount via the
            subjects.
        effects_amount_of_subjects(self): returns a data holder of top total amount of given column name via data frame.

    """

    def __init__(self, logger: Logger.logger, json_data: Dict) -> None:
        """ Initialize Base Class & Instance Attributes """
        super().__init__(logger, json_data)

    def tests_results_by_date(self, date_string: str) -> DefaultDict[str, DefaultDict[str, int]]:
        """ Returns data holder of the corona result's amount by gender via the data frame.

        Args:
            date_string(str): desired date as string.

        Returns:
            data_dict(DefaultDict[str, DefaultDict[str, int]]): test results data in data holder.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            df['test_date'] = df[df['test_date'] == date_string]
            df = df[['corona_result', 'gender']]
            ser_group_by = df.groupby('corona_result')['gender'].value_counts()
            data_dict = defaultdict(lambda: defaultdict(int))
            for key, value in ser_group_by.items():
                # key[0] : test result, key[1] : gender
                data_dict[key[0]][key[1]] = value
        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            return data_dict

    def _get_value_counts_by_column(self, column_name: str) -> Dict[str, int]:
        """ Returns a dictionary of column_name(str): value count(int).

        Args:
            column_name(str): column name for value counting.

        Returns:
            data_dict(Dict[str, int]): amount of test indication in data holder.

        """

        data_dict = None
        try:
            data = self._df[column_name].value_counts()
            data_dict = data.to_dict()
        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            return data_dict

    @lru_cache
    def amount_of_test_indication(self) -> Dict[str, int]:
        """ Returns a dictionary of test_indication: amount of test indication's properties via the subjects.

        Args:
            None.

        Returns:
            data_dict(Dict[str, int]): amount of test indication in data holder.

        """

        return self._get_value_counts_by_column('test_indication')

    @lru_cache
    def amount_of_subjects_ages_60_and_above(self) -> Dict[str, int]:
        """ Returns a dictionary of age group 60+ test status: amount via the subjects.

        Args:
            None.

        Returns:
            _(Dict[str, int]): amount of age group 60+ test status in data holder.

        """
        return self._get_value_counts_by_column('age_60_and_above')

    @lru_cache
    def effects_amount_of_subjects(self) -> Dict[str, Dict[str, int]]:
        """ Returns a data holder of top total amount of symptoms before test date via data frame.

        Args:
            None.

        Returns:
            data_dict(Dict[str, Dict[str, int]]): effects amount of subjects in data holder.

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
            return data_dict

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
