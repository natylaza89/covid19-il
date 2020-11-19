from collections import namedtuple
from functools import lru_cache
from typing import Dict, Generator, NamedTuple, Tuple

from covid19_il.logger.logger import Logger
from covid19_il.data_handler.data_handlers.data_handler import DataHandler


class LabTests(DataHandler):
    """ Covid19_IL Lab Tests Data Handler.

    Attributes:
        None.

    Methods:
        _get_statistics_by_column(self, column_name: str,  is_sorted: bool = False): Yields value counts of given
            column name.
        corona_results(self): Yields value counts of corona results.
        lab_tests_statistics(self): Yields value counts of lab tests.
        is_first_test_statistics(self): Yields value counts of if is it the first test for tested persons.
        test_for_corona_statistics(self): Yields value counts of test_for_corona_statistics.
            '1': tested didn't acknowledge as positive , '0': tested already acknowledged as positive.
        tests_results_data_by_test_date(self, date: str): Yields test results data by given test date as namedtuple.

    """

    fields = ('corona_result', 'lab_id', 'test_for_corona_diagnosis', 'is_first_Test')
    test = namedtuple("CoronaTest", fields, defaults=(None,) * len(fields))

    def __init__(self, logger: Logger.logger, json_data: Dict) -> None:
        """ Initialize Base Class & Instance Attributes """
        super().__init__(logger, json_data)

    def _get_statistics_by_column(self, column_name: str,  is_sorted: bool = False) \
            -> Generator[Dict[str, int], None, None] or Generator[Tuple[str, str], None, None]:
        """ Yields value counts of given column name.

        Note:
            private method - gets called by the others class's methods.

        Args:
            column_name(str): given column name for data manipulation.
            is_sorted(bool): whether sort the results.

        Yields:
            data dict(Generator[Dict[str, int], None, None] or Tuple[str, str]): desired data  or "No Data" for
                bad result.

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
            if bool(data_dict):
                for _item in data_dict.items():
                    yield _item
            else:
                yield "No Data", ""

    @lru_cache
    def corona_results(self) -> Generator[Dict[str, int], None, None] or Generator[Tuple[str, str], None, None]:
        """ Yields value counts of corona results.

        Args:
            None.

        Yields:
            Tuple[str, int] or Tuple[str, str]: desired data or "No Data" for bad result.

        """

        return self._get_statistics_by_column('corona_result', True)

    @lru_cache
    def lab_tests_statistics(self) -> Generator[Dict[str, int], None, None] or Generator[Tuple[str, str], None, None]:
        """ Yields value counts of lab tests.

        Args:
            None.

        Yields:
            Tuple[str, int] or Tuple[str, str]: desired data or "No Data" for bad result.

        """

        return self._get_statistics_by_column('lab_id', True)

    @lru_cache
    def is_first_test_statistics(self) \
            -> Generator[Dict[str, int], None, None] or Generator[Tuple[str, str], None, None]:
        """ Yields value counts of if is it the first test for tested persons.

        Args:
            None.

        Yields:
            Tuple[str, int] or Tuple[str, str]: desired data or "No Data" for bad result.

        """

        return self._get_statistics_by_column('is_first_Test')

    @lru_cache
    def test_for_corona_statistics(self) \
            -> Generator[Dict[str, int], None, None] or Generator[Tuple[str, str], None, None]:
        """ Yields value counts of test_for_corona_statistics.
            '1': tested didn't acknowledge as positive , '0': tested already acknowledged as positive.

        Args:
            None.

        Yields:
            Tuple[str, int] or Tuple[str, str]: desired data or "No Data" for bad result.

        """

        return self._get_statistics_by_column('test_for_corona_diagnosis')

    @lru_cache
    def tests_results_data_by_test_date(self, date: str) \
            -> Generator[NamedTuple, None, None] or Generator[str, None, None]:
        """ Yields test results data by given test date as a namedtuple.

        Args:
            date(str): given date for data manipulation.

        Yields:
            NamedTuple or str: Yields CoronaResult as namedtuple or "No Data" for bad results.

        """

        data = None
        try:
            df = self._get_clean_copy_df_data()
            df = df[df['test_date'] == date]
            ser_group_by = df.groupby([*df.columns])['test_date'].unique()
            data = ser_group_by.keys()
        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            if not data.empty:
                for key in data:
                    yield LabTests.test(*key[2:])
            else:
                yield "No Data"
