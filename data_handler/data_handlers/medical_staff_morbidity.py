from collections import defaultdict
from functools import lru_cache
import re
from typing import Dict, Tuple, Any

from covid19_il.logger.logger import Logger
from covid19_il.data_handler.data_handlers.data_handler import DataHandler


class MedicalStaffMorbidity(DataHandler):
    """ Covid19_IL Medical Staff Morbidity Data Handler.

    Attributes:
        None.

    Methods:
        _get_data_by_columns(self, required_columns_names: Tuple[str, str, str], ascending_order: bool = False):
            Returns dictionary of dictionary with data for data of isolated/confirmed cases.
        confirmed_cases(self): Returns dictionary of dictionary with total confirmed cases data.
        isolated_cases(self): Returns dictionary of dictionary with total isolated cases.
        _get_data_by_date(self, date: str, required_columns_names: Tuple[str, str, str]): Get data by exactly date's
            pattern & returns a dictionary with data statistics.
        confirmed_cases_by_date(self, date: str): Returns confirmed cases statistics by given date as dictionary.
        isolated_cases_by_date(self, date: str): Returns isolated cases statistics by given date as dictionary.
        confirmed_cases_statistics(self): Returns dictionary of dictionary of Confirmed cases statistics: min, max,
            mean, total(sum).
        isolated_cases_statistics(self): Returns dictionary of dictionary of Isolated cases statistics: min, max, mean,
            total(sum).

    """

    confirmed_columns_names = ('confirmed_cases_physicians', 'confirmed_cases_nurses',
                               'confirmed_cases_other_healthcare_workers')
    isolated_columns_names = ('isolated_physicians', 'isolated_nurses', 'isolated_other_healthcare_workers')

    def __init__(self, logger: Logger.logger, json_data: dict) -> None:
        """ Initialize Base Class & Instance Attributes """
        super().__init__(logger, json_data)

    def _get_data_by_columns(self, required_columns_names: Tuple[str, str, str], ascending_order: bool = False)\
            -> Dict[str, Dict[str, Any]]:
        """ Returns dictionary of dictionary with data for data of isolated/confirmed cases.

        Note:
            private method which get called other methods for data manipulation by columns.

        Args:
            required_columns_names(Tuple[str, str, str]): df's columns names for data processing.
            ascending_order(bool): final results order in de/ascending.

        Returns:
            data_dict(Dict[str, Dict[str, Any]]): desired data by date inside a dictionary.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            ser = df.groupby(['Date', *required_columns_names])['Date']
            data = ser.unique()
            data_dict = defaultdict(lambda: dict())
            for key, value in data.items():
                data_dict[key[0]] = \
                    {column_name: data for (column_name, data) in zip(required_columns_names, key[1:])}

        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            return {key: value for (key, value) in sorted(data_dict.items(), reverse=ascending_order)}

    @lru_cache
    def confirmed_cases(self) -> Dict[str, Dict[str, int or str]]:
        """ Returns dictionary of dictionary with total confirmed cases data.

        Args:
            None.

        Returns:
            _(Dict[str, Dict[str, int or str]]): desired data in data holder.

        """

        return self._get_data_by_columns(MedicalStaffMorbidity.confirmed_columns_names)

    @lru_cache
    def isolated_cases(self) -> Dict[str, Dict[str, float or int]]:
        """ Returns dictionary of dictionary with total isolated cases.

        Args:
            None.

        Returns:
            _(Dict[str, Dict[str, float or int]]): desired data in data holder.

        """

        return self._get_data_by_columns(MedicalStaffMorbidity.isolated_columns_names)

    def _get_data_by_date(self, date: str, required_columns_names: Tuple[str, str, str]) -> Dict[str, Any]:
        """ Get data by exactly date's pattern & returns a dictionary with data statistics.

        Note:
            private method which get called other methods for data manipulation by date.

        Args:
            date(str): required date for data processing.
            required_columns_names(Tuple[str, str, str]): df's columns names for data processing.

        Returns:
            data_dict(Dict[str, Any]): desired data by date inside a dictionary.

        Raises:
            ValueError: exception raises when date string isn't in a valid pattern like: "2020-10-03".

        """

        re_result = re.search('(202[0-9])-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$', date)
        if re_result is None:
            self._logger.exception(f"Wrong Date Format, the format should be like: '2020-10-03'. input was: {date}")
            raise ValueError("Wrong Date Format, the format should be like: '2020-10-03'.")

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            df = df[df['Date'] == date]
            ser = df.groupby([*required_columns_names])['Date']
            data = ser.unique()
            data_dict = {column_name: data for (column_name, data) in zip(required_columns_names, *data.keys())}
        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            return data_dict

    @lru_cache
    def confirmed_cases_by_date(self, date: str) -> Dict[str, float or int]:
        """ Return confirmed cases statistics by given date as dictionary.

        Args:
            date(str): desired date for data processing.

        Returns:
            _(Dict[str, float or int]): desired data in data holder.

        """

        return self._get_data_by_date(date, MedicalStaffMorbidity.confirmed_columns_names)

    @lru_cache
    def isolated_cases_by_date(self, date: str) -> Dict[str, str]:
        """ Return isolated cases statistics by given date as dictionary.

        Args:
            date(str): desired date for data processing.

        Returns:
            _(Dict[str, str]): desired data in data holder.

        """

        return self._get_data_by_date(date, MedicalStaffMorbidity.isolated_columns_names)

    @lru_cache
    def confirmed_cases_statistics(self) -> Dict[str, Dict[str, int or float]]:
        """ Return dictionary of dictionary of Confirmed cases statistics: min, max, mean, total(sum).

        Args:
            None.

        Returns:
            _(Dict[str, Dict[str, int or float]]): desired data in data holder.

        """

        return self._get_statistics_by_columns_names(MedicalStaffMorbidity.confirmed_columns_names)

    @lru_cache
    def isolated_cases_statistics(self) -> Dict[str, Dict[str, int or float]]:
        """ Return dictionary of dictionary of Isolated cases statistics: min, max, mean, total(sum).

        Args:
            None.

        Returns:
            _(Dict[str, Dict[str, int or float]]): desired data in data holder.

        """

        return self._get_statistics_by_columns_names(MedicalStaffMorbidity.isolated_columns_names)
