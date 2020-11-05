from collections import defaultdict
from functools import lru_cache
from typing import Dict, DefaultDict

from covid19_il.logger.logger import Logger
from covid19_il.data_handler.data_handlers.data_handler import DataHandler


class TestedIndividualsScores(DataHandler):
    """ Covid19_IL Tested Individuals Scores Data Handler.

    Attributes:
        None.

    Methods:
        get_statistics(self): Returns statistics in gender groups with its age group amount value.
        get_statistics_by_date(self, date_string: str): Returns data holder of statistic by given date.

    """

    def __init__(self, logger: Logger.logger, json_data: Dict) -> None:
        """ Initialize Base Class & Instance Attributes """
        super().__init__(logger, json_data)

    def get_statistics(self) -> DefaultDict[str, DefaultDict[str, int]]:
        """ Returns statistics in gender groups with its age group amount value.
        Args:
            None.

        Returns:
            data dict(DefaultDict[str, DefaultDict[str, int]]): desired data inside data holder.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            data_dict = defaultdict(lambda: defaultdict(int))

            for column in df.columns[2:]:
                temp_df = df[['age_60_and_above', column]]
                ser_group_by = temp_df.groupby(['age_60_and_above', column])['age_60_and_above'].unique()
                for key, _ in ser_group_by.items():
                    # key[0]: age_60_and_above, key[1]: value amount
                    data_dict[column][key[0]] += int(key[1])

        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            return data_dict

    def get_statistics_by_date(self, date_string: str) -> DefaultDict[str, Dict[str, int]]:
        """ Returns data holder of statistic by given date.

        Args:
            date_string(str): desired date as string.

        Returns:
            data_dict(DefaultDict[str, Dict[str, int]]): test results data in data holder.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            df = df[df['test_date'] == date_string]
            ser_group_by = df.groupby([*df.columns])['age_60_and_above'].unique()
            data_dict = defaultdict(lambda: dict())
            for key in ser_group_by.keys():
                # key[0]: age_60_and_above, key[1]: value amount
                data_dict[key[1]] = {gender: int(value) for gender, value in zip(df.columns[2:], key[2:])}

        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            return data_dict
