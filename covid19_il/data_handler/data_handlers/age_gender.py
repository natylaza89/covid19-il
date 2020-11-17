from collections import defaultdict
from functools import lru_cache
from typing import Dict, DefaultDict, Tuple

from covid19_il.logger.logger import Logger
from covid19_il.data_handler.data_handlers.data_handler import DataHandler


class AgeGender(DataHandler):
    """ Covid19_IL Cities Data Handler.

    Attributes:
        None.

    Methods:
        cities_by_date(self, date: str = dt.strftime(dt.now(), format="%Y-%m-%d")): return calculated dictionary of
            city_name: namedtuple with city's data props via given date
        statistics_by_gender(self): Returns statistic data via first week day with results which grouped by gender.
        _get_statistics_by_columns_names(self, columns_names: Tuple): private method - Returns statistic data via first
            week day with results which grouped by gender.
        statistics_by_given_first_week_day(self, week_day: str): Returns statistic data by given first week day grouped
            by gender.
        statistics_by_age_group(self):Returns statistic of calculated fields by ordered by age group.

    """

    calculated_fields = ('weekly_tests_num', 'weekly_newly_tested', 'weekly_cases', 'weekly_deceased')

    def __init__(self, logger: Logger.logger, json_data: Dict) -> None:
        """ Initialize Base Class & Instance Attributes """
        super().__init__(logger, json_data)

    @lru_cache
    def statistics_by_gender(self) -> DefaultDict[str, DefaultDict[str, Dict[str, Dict[str, int or str]]]]:
        """ Returns statistic data via first week day with results which grouped by gender.

        Args:
            None.

        Returns:
            _(DefaultDict[str, DefaultDict[str, Dict[str, str]]]): desired data inside data holder.

        """

        return self._get_statistics_by_columns_names(AgeGender.calculated_fields)

    def _get_statistics_by_columns_names(self, columns_names: Tuple) \
            -> DefaultDict[str, DefaultDict[str, Dict[str, Dict[str, int or str]]]]:
        """ Returns statistic data via first week day with results which grouped by gender.

        Note:
            private method which get called by other method for data statistic calculation.

        Args:
            columns_names(Tuple): required columns name of data frames for data manipulations.

        Returns:
            data dict: (DefaultDict[str, DefaultDict[str, Dict[str, Dict[int or str]]]]): desired data inside data holder.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            ser = df.groupby([*df.columns])['gender']
            data = ser.unique()
            data_dict = defaultdict(lambda: defaultdict(lambda: dict()))
            for key in data.keys():
                # key[0]: first week day, key[2]: age_group, key[3]: gender, key[4:]: the actual values
                data_dict[key[3]][key[0]][key[2]] = {key: value for key, value in zip(columns_names, key[4:])}
                # data_dict[key[0]][key[3]][key[2]] = {key: value for key, value in zip(columns_names[1:], key[4:])}
        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            return data_dict

    @lru_cache
    def statistics_by_given_first_week_day(self, week_day: str) -> DefaultDict[str, Dict[str, Dict[str, str]]]:
        """ Returns statistic data by given first week day grouped by gender.

        Args:
            week_day(str): given week day for data manipulation.

        Returns:
            data dict(DefaultDict[str, Dict[str, Dict[str, str]]]): desired data inside data holder.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            df = df[df['first_week_day'] == week_day]
            ser = df.groupby([*df.columns])['gender']
            data = ser.unique()
            data_dict = defaultdict(lambda: dict())
            for key in data.keys():
                # key[0]: first week day, key[2]: age_group, key[3]: gender, key[4:]: the actual values
                data_dict[key[3]][key[2]] = {key: value for key, value in zip(AgeGender.calculated_fields, key[4:])}
        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            return data_dict

    @lru_cache
    def statistics_by_age_group(self) -> DefaultDict[str, Dict[str, int or float]]:
        """ Returns statistic of calculated fields by ordered by age group.

        Args:
            None.

        Returns:
            data dict(DefaultDict[str, Dict[str, int or float]]): desired data inside data holder.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            for column_name in AgeGender.calculated_fields:
                df[column_name] = [self._convert_string_to_int(item) for item in df[column_name]]
            age_groups = df['age_group'].unique()
            data_dict = defaultdict(lambda: dict())

            for group in age_groups:
                temp_df = df[df['age_group'] == group]
                for field in AgeGender.calculated_fields:
                    df_by_field = temp_df[field]
                    data_dict[group][field] = {"min": df_by_field.min(),
                                               "max": df_by_field.max(),
                                               "mean": df_by_field.mean(),
                                               "total": df_by_field.sum()}
        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            return data_dict
