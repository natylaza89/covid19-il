from collections import defaultdict, namedtuple
from functools import lru_cache
from typing import Dict, Generator, NamedTuple, AnyStr

from covid19_il.logger.logger import Logger
from covid19_il.data_handler.data_handlers.data_handler import DataHandler


class LabTests(DataHandler):
    """ Covid19_IL Lab Tests Data Handler.

    Attributes:
        None.

    Methods:
        corona_results(self): Returns value counts of corona results.
        lab_tests_statistics(self): Returns value counts of lab tests.
        is_first_test_statistics(self): Returns value counts of if is it the first test for tested persons.
        test_for_corona_statistics(self): Returns value counts of test_for_corona_statistics.
            '1': tested didn't acknowledge as positive , '0': tested already acknowledged as positive.


    """

    fields = ('corona_result', 'lab_id', 'test_for_corona_diagnosis', 'is_first_Test')
    test = namedtuple("CoronaTest", fields, defaults=(None,) * len(fields))

    def __init__(self, logger: Logger.logger, json_data: Dict) -> None:
        """ Initialize Base Class & Instance Attributes """
        super().__init__(logger, json_data)

    def _get_statistics_by_column(self, column_name: str,  is_sorted: bool = False) -> Dict[str, int]:
        """ Returns value counts of given column name.

        Note:
            private method - gets called by the others class's methods.

        Args:
            column_name(str): given column name for data manipulation.
            is_sorted(bool): whether sort the results.

        Returns:
            data dict(Dict[str, int]): desired data inside data holder.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            df = df[column_name]
            ser = df.value_counts()
            if is_sorted:
                data_dict = {key: value for key, value in sorted(ser.items(), key=lambda item: item[1], reverse=True)}
            else:
                data_dict = ser.to_dict()

        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            return data_dict

    @lru_cache
    def corona_results(self) -> Dict[str, int]:
        """ Returns value counts of corona results.

        Args:
            None.

        Returns:
            _(Dict[str, int]): desired data inside data holder.

        """

        return self._get_statistics_by_column('corona_result', True)

    @lru_cache
    def lab_tests_statistics(self) -> Dict[str, int]:
        """ Returns value counts of lab tests.

        Args:
            None.

        Returns:
            _(Dict[str, int]): desired data inside data holder.

        """

        return self._get_statistics_by_column('lab_id', True)

    @lru_cache
    def is_first_test_statistics(self) -> Dict[str, int]:
        """ Returns value counts of if is it the first test for tested persons.

        Args:
            None.

        Returns:
            _(Dict[str, int]): desired data inside data holder.

        """

        return self._get_statistics_by_column('is_first_Test')

    @lru_cache
    def test_for_corona_statistics(self) -> Dict[str, int]:
        """ Returns value counts of test_for_corona_statistics.
            '1': tested didn't acknowledge as positive , '0': tested already acknowledged as positive

        Args:
            None.

        Returns:
            _(Dict[str, int]): desired data inside data holder.

        """

        return self._get_statistics_by_column('test_for_corona_diagnosis')

    @lru_cache
    def tests_results_data_by_test_date(self, date: str) -> Generator[NamedTuple, None, None]:
        """ Returns test results data by given test date as generator of namedtuples.

        Args:
            date(str): given date for data manipulation.

        Yields:
            _Generator[NamedTuple, None, None]: yield CoronaResult namedtuple each time.

        """

        try:
            df = self._get_clean_copy_df_data()
            df = df[df['test_date'] == date]
            ser_group_by = df.groupby([*df.columns])['test_date'].unique()

            for key in ser_group_by.keys():
                yield LabTests.test(*key[2:])

        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")

