from collections import defaultdict
from functools import lru_cache
from typing import Dict, DefaultDict, Generator

from covid19_il.logger.logger import Logger
from covid19_il.data_handler.data_handlers.data_handler import DataHandler


class Deaths(DataHandler):
    """ Covid19_IL Deaths Data Handler.

    Attributes:
        None.

    Methods:
        amount_of_deaths(self): Yields amount of deaths data.
        amount_of_ventilated(self): Yields amount of ventilated/unventilated by age group & gender data.
        _get_data_by_column(self, group_by_column: str, ascending_order: bool = False): Yields amount of given column
            grouped by age group data
        time_between_positive_and_hospitalization(self): Yields amount of time between positive and hospitalization by
            age group & gender data.
        length_of_hospitalization(self): Yields length of hospitalization's amount by age group & gender data.
        time_between_positive_and_death(self): Yields time between positive and death amount by age group & gender data.

    """

    def __init__(self, logger: Logger.logger, json_data: Dict) -> None:
        """ Initialize Base Class & Instance Attributes """
        super().__init__(logger, json_data)

    @lru_cache
    def amount_of_deaths(self) \
            -> Generator[DefaultDict[str, DefaultDict[str, int]], None, None] or Generator[str, None, None]:
        """ Yields amount of deaths data.

        Args:
            None.

        Yields:
            Tuple[str, DefaultDict[str, int]] or str: desired data or a "No Data" string for bad result.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            df = df[['gender', 'age_group']]
            data = df.value_counts()
            data_dict = defaultdict(lambda: defaultdict(int))

            for key, value in data.items():
                # key[0]: gender, key[1]: age_group
                data_dict[key[0]][key[1]] = value
        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            if bool(data_dict):
                yield from data_dict.items()
            else:
                yield "No Data"

    @lru_cache
    def amount_of_ventilated(self) -> \
            Generator[DefaultDict[str, DefaultDict[str, DefaultDict[str, int]]], None, None] or \
            Generator[str, None, None]:
        """ Yields amount of ventilated/unventilated by age group & gender data.

        Args:
            None.

        Yields:
            Tuple[str, DefaultDict[str, DefaultDict[str, int]]] or str: desired data or "No Data" for bad result.

        """

        return self._get_data_by_columns(('gender', 'age_group', 'Ventilated'), 'age_group')

    def _get_data_by_column(self, group_by_column: str, ascending_order: bool = False) \
            -> Generator[DefaultDict[str, DefaultDict[str, int]], None, None] or Generator[str, None, None]:
        """ Yields amount of given column grouped by age group data.

        Note:
            private methods which get called by other methods for calculation.

        Args:
            group_by_column(str): column name of event type.
            ascending_order(bool): final result's ordering by de/ascending.

        Yields:
             Tuple[str, DefaultDict[str, int]] or str: desired data or "No Data" for bad result.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            df = df[['age_group', group_by_column]]
            ser_group_by = df.groupby(['age_group'])[group_by_column]
            data_from_series = ser_group_by.value_counts().to_dict()
            data_dict = defaultdict(lambda: defaultdict(int))

            for key, value in data_from_series.items():
                data_dict[key[0]][key[1]] = value
        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            if bool(data_dict):
                yield from data_dict.items()
            else:
                yield "No Data"

    @lru_cache
    def time_between_positive_and_hospitalization(self) \
            -> Generator[DefaultDict[str, DefaultDict[str, int]], None, None] or Generator[str, None, None]:
        """ Yields amount of time between positive and hospitalization by age group & gender data.

        Args:
            None.

        Yields:
             Tuple[str, DefaultDict[str, int]] or str: desired data or "No Data" for bad result.

        """

        return self._get_data_by_column('Time_between_positive_and_hospitalization')

    @lru_cache
    def length_of_hospitalization(self) \
            -> Generator[DefaultDict[str, DefaultDict[str, int]], None, None] or Generator[str, None, None]:
        """ Yields length of hospitalization's amount by age group & gender data.

        Args:
            None.

       Yields:
             Tuple[str, DefaultDict[str, int]] or str: desired data or "No Data" for bad result.

        """

        return self._get_data_by_column('Length_of_hospitalization')

    @lru_cache
    def time_between_positive_and_death(self) \
            -> Generator[DefaultDict[str, DefaultDict[str, int]], None, None] or Generator[str, None, None]:
        """ Yields time between positive and death amount by age group & gender data.

        Args:
            None.

        Yields:
             Tuple[str, DefaultDict[str, int]] or str: desired data or "No Data" for bad result.

        """

        return self._get_data_by_column('Time_between_positive_and_death')
