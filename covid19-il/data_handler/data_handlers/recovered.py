from collections import defaultdict
from functools import lru_cache
from numpy import int64 as numpy_int64, float64 as numpy_float64
from typing import Dict, DefaultDict

from covid19_il.logger.logger import Logger
from covid19_il.data_handler.data_handlers.data_handler import DataHandler


class Recovered(DataHandler):
    """ Covid19_IL Recovered Data Handler.

    Attributes:
        None.

    Methods:
        test_indication(self): returns data holder of test indication's amount by gender & age group.
        days_from_pos_to_recovery_stats(self): Min, Max, Mean of Days from positive to recovery.
        total_tests_count(self): Returns total tests count by gender & age groups.

    """

    def __init__(self, logger: Logger.logger, json_data: Dict) -> None:
        """ Initialize Base Class & Instance Attributes """
        super().__init__(logger, json_data)

    @lru_cache
    def test_indication(self) -> DefaultDict[str, DefaultDict[str, DefaultDict[str, int]]]:
        """ Returns data holder of test indication's amount by gender & age group.

        Args:
            None.

        Returns:
            _(DefaultDict[str, DefaultDict[str, DefaultDict[str, int]]]): desired data in data holder.

        """

        return self._get_data_by_columns(('test_indication', 'gender', 'age_group'), 'age_group')

    @lru_cache
    def days_from_pos_to_recovery_stats(self) -> Dict[str, numpy_int64 or numpy_float64]:
        """ Min, Max, Mean of Days from positive to recovery.

        Args:
            None.

        Returns:
            _(Dict): desired data in data holder.

        Raises:
            KeyError: concrete error which can occurred if data frame can't be access by given key.
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
            return data_dict

    @lru_cache
    def total_tests_count(self) -> DefaultDict[str, DefaultDict[str, int]]:
        """ Returns total tests count by gender & age groups.

        Args:
            None.

        Returns:
            _(DefaultDict[str, DefaultDict[str, int]]): desired data in data holder.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            df = df[['total_tests_count', 'gender', 'age_group']]
            ser_group_by = df.groupby(['total_tests_count', 'gender'])['age_group'].value_counts()
            data_dict = defaultdict(lambda: defaultdict(int))

            for key, value in ser_group_by.items():
                # key[0]: test_amount, key[1] - gender, key[2]: age_group
                data_dict[key[2]][key[1]] += (value * int(key[0]))

        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            return data_dict

