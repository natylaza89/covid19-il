from collections import defaultdict
from functools import lru_cache
from typing import Dict, DefaultDict

from covid19_il.logger.logger import Logger
from covid19_il.data_handler.data_handlers.data_handler import DataHandler


class Deaths(DataHandler):
    """ Covid19_IL Deaths Data Handler.

    Attributes:
        None.

    Methods:
        _get_data_by_column(self, group_by_column: str, ascending_order: bool = False): return amount of given column.
        amount_of_deaths(self): Return amount of deaths in  data holder.
        amount_of_ventilated(self): returns amount of ventilated/unventilated by age group & gender.
        time_between_positive_and_hospitalization(self): returns amount of time between positive and hospitalization
            by age group & gender.
        length_of_hospitalization(self): returns length of hospitalization's amount by age group & gender.
        time_between_positive_and_death(self): returns time between positive and death amount by age group & gender.

    TODO
        1. can be added global min, max, mean/average

    """
    def __init__(self, logger: Logger.logger, json_data: Dict) -> None:
        """ Initialize Base Class & Instance Attributes """
        super().__init__(logger, json_data)

    def _get_data_by_column(self, group_by_column: str, ascending_order: bool = False) \
            -> DefaultDict[str, DefaultDict[str, int]]:
        """ Return amount of given column.

        Note:
            private methods which get called by other methods for calculation.

        Args:
            group_by_column(str): column name of event type.
            ascending_order(bool): final result's ordering by de/ascending.

        Returns:
            data_dict(DefaultDict[str, DefaultDict[str, int]]): desired data in data holder.

        Raises:
            KeyError: concrete error which can occurred if data frame can't be access by given key.
        """

        data_dict = None
        try:
            df = self.df.copy()
            df = df[['age_group', group_by_column]]
            ser = df.groupby(['age_group'])[group_by_column]
            data_from_series = ser.value_counts().to_dict()
            data_dict = defaultdict(lambda: defaultdict(int))

            for key, value in data_from_series.items():
                data_dict[key[0]][key[1]] = value
        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
            # TODO: check in tests if exception raise
            raise
        finally:
            return data_dict

    @lru_cache
    def amount_of_deaths(self) -> DefaultDict[str, DefaultDict[str, int]]:
        """ Return amount of deaths in  data holder.

        Args:
            None.

        Returns:
            data_dict(DefaultDict[str, DefaultDict[str, int]]): desired data in data holder.

        Raises:
            KeyError: concrete error which can occurred if data frame can't be access by given key.
        """

        data_dict = None
        try:
            df = self.df[['gender', 'age_group']]
            data = df.value_counts()
            data_dict = defaultdict(lambda: defaultdict(int))

            for key, value in data.items():
                data_dict[key[0]][key[1]] = value
        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
            # TODO: check in tests if exception raise
            raise
        finally:
            return data_dict

    @lru_cache
    def amount_of_ventilated(self) -> DefaultDict[str, DefaultDict[str, DefaultDict[str, int]]]:
        """ Returns amount of ventilated/unventilated by age group & gender.

        Args:
            None.

        Returns:
            _(DefaultDict[str, DefaultDict[str, int]]): desired data in data holder.

        """

        return self._get_data_by_columns(('gender', 'age_group', 'Ventilated'), 'age_group')

    @lru_cache
    def time_between_positive_and_hospitalization(self) -> DefaultDict[str, DefaultDict[str, int]]:
        """ Returns amount of time between positive and hospitalization by age group & gender.

        Args:
            None.

        Returns:
            _(DefaultDict[str, DefaultDict[str, int]]): desired data in data holder.

        """

        return self._get_data_by_column('Time_between_positive_and_hospitalization')

    @lru_cache
    def length_of_hospitalization(self) -> DefaultDict[str, DefaultDict[str, int]]:
        """ Returns length of hospitalization's amount by age group & gender.

        Args:
            None.

        Returns:
            _(DefaultDict[str, DefaultDict[str, int]]): desired data in data holder.

        """

        return self._get_data_by_column('Length_of_hospitalization')

    @lru_cache
    def time_between_positive_and_death(self) -> DefaultDict[str, DefaultDict[str, int]]:
        """ Returns time between positive and death amount by age group & gender.

        Args:
            None.

        Returns:
            _(DefaultDict[str, DefaultDict[str, int]]): desired data in data holder.

        """

        return self._get_data_by_column('Time_between_positive_and_death')

