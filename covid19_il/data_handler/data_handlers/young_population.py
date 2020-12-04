from collections import defaultdict
from functools import lru_cache
from typing import Dict, DefaultDict, Tuple, Any, Generator

from covid19_il.logger.logger import Logger
from covid19_il.data_handler.data_handlers.data_handler import DataHandler


class YoungPopulation(DataHandler):
    """ covid19_il Young Population Data Handler.

    Attributes:
        None.

    Methods:
        _get_statistics_by_columns_names(self, columns_names: Tuple[str, str, str]): Yields statistics by given columns
            names inside data holder.
        total_cases_statistics(self): Yields Confirmed cases statistics inside data holder.
        _get_data_by_columns(self, required_columns_names: Tuple[str, str, str], key_column_name: str = 'region'):
            Yields data statistics by given key column name.
        cases_statistics_by_region(self): Yields data of cases statistics(min, max, mean, sum) by region.
        cases_statistics_by_age_group(self): Yields data of cases statistics(min, max, mean, sum) by age group.
        cases_statistics_by_first_week_day(self): Yields data of cases statistics(min, max, mean, sum) by first week
            day.
    """

    required_columns_names = ('weekly_tests_num', 'weekly_newly_tested', 'weekly_cases')

    def __init__(self, logger: Logger.logger, json_data: dict) -> None:
        """ Initialize Base Class & Instance Attributes """
        super().__init__(logger, json_data)

    def _get_statistics_by_columns_names(self, columns_names: Tuple[str, str, str]) \
            -> Generator[DefaultDict[str, DefaultDict[str, DefaultDict[str, Dict[str, int]]]], None, None] or \
               Generator[Tuple[str, str], None, None]:
        """ Yields statistics by given columns names inside data holder.

        Note:
            private method which get called by other methods for statistics calculation by given columns.

        Args:
            columns_names(Tuple[str, str, str]): given columns name of data frame for calculation.

        Yields:
            Yields {period:
                        {region:
                            {age_group:
                                {weekly_tests_num: 3,
                                 weekly_newly_tested: 5,
                                  weekly_cases: 1 }}}}

        """

        data_dict = None

        try:
            df = self._get_clean_copy_df_data()
            for column_name in columns_names:
                df[column_name] = [self._convert_string_to_int(item) for item in df[column_name]]
            ser = df.groupby(['first_week_day', 'region', 'age_group', *columns_names])['first_week_day']
            data = ser.unique()

            data_dict = defaultdict(lambda:defaultdict(lambda: defaultdict(dict)))

            for key, value in data.items():
                # key[0]: date, key[1]: region, key[2]:age_group key[3,4,5]: calculated props
                cleaned_date_key = key[0].split('T')[0]
                data_dict[cleaned_date_key][key[1]][key[2]] = {key: value for key, value in
                                                               zip(columns_names, (key[3], key[4], key[5]))}
        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            if bool(data_dict):
                yield from data_dict.items()
            else:
                yield "No Data", ""

    @lru_cache
    def total_cases_statistics(self) \
            -> Generator[DefaultDict[str, DefaultDict[str, DefaultDict[str, Dict[str, int]]]], None, None] or \
               Generator[Tuple[str, str], None, None]:
        """ Yields Confirmed cases statistics inside data holder.

        Args:
            None.

        Yields:
           Tuple[str, DefaultDict[str, DefaultDict[str, Dict[str, int]]]] or Tuple[str, str]: desired data or "No Data"
               as bad result.

        """

        return self._get_statistics_by_columns_names(YoungPopulation.required_columns_names)

    def _get_data_by_columns(self, required_columns_names: Tuple[str, str, str], key_column_name: str = 'region')\
            -> Dict[str, Dict[str, Any]]:
        """ Yields data statistics by given key column name.

        Note:
            private method which get called by other methods for data calculation by given columns.

        Args:
            required_columns_names(Tuple[str, str, str]): given columns names for df manipulation.
            key_column_name(str): name of the key column for data manipulation by it.

        Yields:
            Tuple[str, Dict[str, Dict[str, int]]] or Tuple[str, str]: desired data or "No Data" as bad result.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            # parsing string values to integers for future calculations
            for column_name in required_columns_names:
                df[column_name] = [self._convert_string_to_int(item) for item in df[column_name]]
            key_column_items_names = df[key_column_name].unique()
            data_dict = defaultdict(lambda: dict())
            # build the main default dict with data by key_column_items_names(region, age_group,first_week_day)
            # by the 3 required columns with the converted data as "prop".
            _ = {data_dict[column_key].update(self._statistics_operation_to_dict(df, prop, column_key, key_column_name))
                 for column_key in key_column_items_names
                 for prop in required_columns_names}
        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            if bool(data_dict):
                yield from data_dict.items()
            else:
                yield "No Data", ""

    def _statistics_operation_to_dict(self, df, prop, column_key, key_column_name) \
            -> Dict[str, Dict[str, int or float]]:
        current_df = df[df[key_column_name] == column_key]
        return {prop: {"min": current_df[prop].min(),
                       "max": current_df[prop].max(),
                       "mean": current_df[prop].mean(),
                       "total": current_df[prop].sum()
                       }
                }

    @lru_cache
    def cases_statistics_by_region(self) -> Dict[str, Dict[str, int or float]]:
        """ Yields data of cases statistics(min, max, mean, sum) by region.

        Args:
            None.

        Yields:
            Tuple[str, Dict[str, Dict[str, int]]] or Tuple[str, str]: desired data or "No Data" as bad result.

        """

        return self._get_data_by_columns(YoungPopulation.required_columns_names, key_column_name='region')

    @lru_cache
    def cases_statistics_by_age_group(self) -> Dict[str, Dict[str, int or float]]:
        """ Yields data of cases statistics(min, max, mean, sum) by age group.

        Args:
            None.

        Yields:
            Tuple[str, Dict[str, Dict[str, int]]] or Tuple[str, str]: desired data or "No Data" as bad result.

        """

        return self._get_data_by_columns(YoungPopulation.required_columns_names, key_column_name='age_group')

    @lru_cache
    def cases_statistics_by_first_week_day(self) -> Dict[str, Dict[str, int or float]]:
        """ Yields data of cases statistics(min, max, mean, sum) by first week day.

        Args:
            None.

        Yields:
            Tuple[str, Dict[str, Dict[str, int]]] or Tuple[str, str]: desired data or "No Data" as bad result.

        """

        return self._get_data_by_columns(YoungPopulation.required_columns_names, key_column_name='first_week_day')
