from collections import defaultdict
from functools import lru_cache
from numpy import int64 as numpy_int64, float64 as numpy_float64
from typing import Dict, DefaultDict, Generator, Tuple

from covid19_il.logger.logger import Logger
from covid19_il.data_handler.data_handlers.data_handler import DataHandler


class Recovered(DataHandler):
    """ Covid19_IL Recovered Data Handler.

    Attributes:
        None.

    Methods:
        test_indication(self): Yields test indication's amount by gender & age group.
        days_from_pos_to_recovery_stats(self): Min, Max, Mean of Days from positive to recovery.
        total_tests_count(self): Returns total tests count by gender & age groups.

    """

    def __init__(self, logger: Logger.logger, json_data: Dict) -> None:
        """ Initialize Base Class & Instance Attributes """
        super().__init__(logger, json_data)

    @lru_cache
    def test_indication(self) -> Generator[DefaultDict[str, DefaultDict[str, DefaultDict[str, int]]], None, None] or \
                                 Generator[Tuple[str, str], None, None]:

        """ Yields test indication's amount by gender & age group.

        Args:
            None.

        Yields:
            Tuple[str, DefaultDict[str, DefaultDict[str, int]]] or Tuple[str, str]): desired data or "No Data" as
                bad result.

        """

        return self._get_data_by_columns(('test_indication', 'gender', 'age_group'), 'age_group')

    @lru_cache
    def days_from_pos_to_recovery_stats(self) \
            -> Generator[Dict[str, numpy_int64 or numpy_float64], None, None] or \
               Generator[Tuple[str, str], None, None]:
        """ Min, Max, Mean of Days from positive to recovery.

        Args:
            None.

       Yields:
            Tuple[str, numpy_int64 or numpy_float64] or Tuple[str, str]: desired data or "No Data" as bad result.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            df = df[['days_between_pos_and_recovery']]
            df['days_between_pos_and_recovery'] = [self._convert_string_to_int(item[0]) for item
                                                   in df['days_between_pos_and_recovery']]
            data_dict = {"min": int(df.min().values[0]),
                         "max": int(df.max().values[0]),
                         "mean": float(df.mean().values[0])}

        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            if bool(data_dict):
                yield from data_dict.items()
            else:
                yield "No Data", ""

    @lru_cache
    def total_tests_count(self) -> Generator[DefaultDict[str, DefaultDict[str, Dict[str, int]]], None, None] or \
                                   Generator[Tuple[str, str], None, None]:
        """ Returns total tests count by gender & age groups.

        Args:
            None.

        Yields:
            Tuple[str, DefaultDict[str, Dict[str, int]] or Tuple[str, str]: hospitalized_total_stats's data or
             "No Data" as  bad result.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            df = df[['total_tests_count', 'gender', 'age_group']]
            ser_group_by = df.groupby(['total_tests_count', 'gender'])['age_group'].value_counts()
            data_dict = defaultdict(lambda: defaultdict(int))

            for key, value in ser_group_by.items():
                # key[0]: test_amount, key[1] - gender, key[2]: age_group
                test_amount = 0 if key[0] == 'NULL' else int(key[0].strip('+'))
                data_dict[key[2]][key[1]] += (value * test_amount)

        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            if bool(data_dict):
                yield from data_dict.items()
            else:
                yield "No Data", ""

