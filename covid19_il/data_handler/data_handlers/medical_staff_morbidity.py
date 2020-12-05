from collections import defaultdict
from functools import lru_cache
from typing import Dict, Tuple, Any, Generator

from covid19_il.logger.logger import Logger
from covid19_il.data_handler.data_handlers.data_handler import DataHandler


class MedicalStaffMorbidity(DataHandler):
    """ Covid19_IL Medical Staff Morbidity Data Handler.

    Attributes:
        None.

    Methods:
        _get_data_by_columns(self, required_columns_names: Tuple[str, str, str], ascending_order: bool = False):
            Yields data for data of isolated/confirmed cases.
        confirmed_cases(self): Yields total confirmed cases data.
        isolated_cases(self): Yields total isolated cases.
        _get_data_by_date(self, date: str, required_columns_names: Tuple[str, str, str]): Get data by exactly date's
            pattern & Yields data statistics.
        confirmed_cases_by_date(self, date: str): Yields confirmed cases statistics by given date.
        isolated_cases_by_date(self, date: str): Yields isolated cases statistics by given date.
        confirmed_cases_statistics(self): Yields Confirmed cases statistics: min, max, mean, total(sum).
        isolated_cases_statistics(self): Yields Isolated cases statistics: min, max, mean, total(sum).

    """

    confirmed_columns_names = ('confirmed_cases_physicians', 'confirmed_cases_nurses',
                               'confirmed_cases_other_healthcare_workers')
    isolated_columns_names = ('isolated_physicians', 'isolated_nurses', 'isolated_other_healthcare_workers')

    def __init__(self, logger: Logger.logger, json_data: dict) -> None:
        """ Initialize Base Class & Instance Attributes """
        super().__init__(logger, json_data)

    def _get_data_by_columns(self, required_columns_names: Tuple[str, str, str], ascending_order: bool = False)\
            -> Generator[Dict[str, Dict[str, Any]], None, None] or Generator[Tuple[str, str], None, None]:
        """ Yields data of isolated/confirmed cases.

        Note:
            private method which get called other methods for data manipulation by columns.

        Args:
            required_columns_names(Tuple[str, str, str]): df columns names for data processing.
            ascending_order(bool): final results order in de/ascending.

        Yields:
            Tuple[str, Dict[str, int or str]] or Tuple[str, str]: desired data or "No Data" as bad result.

        """

        data_dict = None
        try:
            df = self._get_clean_copy_df_data()
            ser = df.groupby(['Date', *required_columns_names])['Date']
            data = ser.unique()
            data_dict = defaultdict(lambda: dict())

            for key, value in data.items():
                data_dict[key[0]] = {column_name: data for (column_name, data) in zip(required_columns_names, key[1:])}

            data_dict = {key: value for (key, value) in sorted(data_dict.items(), reverse=ascending_order)}
        except KeyError as ke:
            self._logger.exception(ke, "No DataFrame's key exists according to the api client's query results")
        finally:
            if bool(data_dict):
                yield from data_dict.items()
            else:
                yield "No Data", ""

    @lru_cache
    def confirmed_cases(self) \
            -> Generator[Dict[str, Dict[str, int or str]], None, None] or Generator[Tuple[str, str], None, None]:
        """ Yields total confirmed cases data.

        Args:
            None.

        Yields:
            Tuple[str, Dict[str, int or str]] or Tuple[str, str]: desired data or "No Data" as bad result.

        """

        return self._get_data_by_columns(MedicalStaffMorbidity.confirmed_columns_names)

    @lru_cache
    def isolated_cases(self) \
            -> Generator[Dict[str, Dict[str, int or str]], None, None] or Generator[Tuple[str, str], None, None]:
        """ Yields total isolated cases.

        Args:
            None.

        Yields:
            Tuple[str, Dict[str, int or str]]: desired data or "No Data" as bad result.

        """

        return self._get_data_by_columns(MedicalStaffMorbidity.isolated_columns_names)

    def _get_data_by_date(self, date: str, required_columns_names: Tuple[str, str, str]) \
            -> Generator[Dict[str, Any], None, None] or Generator[Tuple[str, str], None, None]:
        """ Get data by exactly date's pattern & Yields data statistics.

        Note:
            private method which get called other methods for data manipulation by date.

        Args:
            date(str): required date for data processing.
            required_columns_names(Tuple[str, str, str]): df columns names for data processing.

        Yields:
            Tuple[str, Any] or Tuple[str, str]): desired data or "No Data" as bad result.

        Raises:
            ValueError: exception raises when date string isn't in a valid pattern like: "2020-10-03".

        """

        self._date_validation(pattern='(202[0-9])-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$',
                              date=date)

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
            if bool(data_dict):
                yield from data_dict.items()
            else:
                yield "No Data", ""

    @lru_cache
    def confirmed_cases_by_date(self, date: str) \
            -> Generator[Dict[str, Any], None, None] or Generator[Tuple[str, str], None, None]:
        """ Yields confirmed cases statistics by given date.

        Args:
            date(str): desired date for data processing.

        Yields:
            Tuple[str, Any] or Tuple[str, str]): desired data or "No Data" as bad result.

        """

        return self._get_data_by_date(date, MedicalStaffMorbidity.confirmed_columns_names)

    @lru_cache
    def isolated_cases_by_date(self, date: str) \
            -> Generator[Dict[str, str], None, None] or Generator[Tuple[str, str], None, None]:
        """ Yields isolated cases statistics by given date.

        Args:
            date(str): desired date for data processing.

        Yields:
            Tuple[str, Any] or Tuple[str, str]): desired data or "No Data" as bad result.

        """

        return self._get_data_by_date(date, MedicalStaffMorbidity.isolated_columns_names)

    @lru_cache
    def confirmed_cases_statistics(self) \
            -> Generator[Dict[str, Dict[str, int or float]], None, None] or Generator[Tuple[str, str], None, None]:
        """ Yields Confirmed cases statistics: min, max, mean, total(sum).

        Args:
            None.

        Yields:
            Tuple[str, Dict[str, int or float]] or Tuple[str, str]): desired data or "No Data" as bad result.

        """

        return self._get_statistics_by_columns_names(MedicalStaffMorbidity.confirmed_columns_names)

    @lru_cache
    def isolated_cases_statistics(self) \
            -> Generator[Dict[str, Dict[str, int or float]], None, None] or Generator[Tuple[str, str], None, None]:
        """ Yields Isolated cases statistics: min, max, mean, total(sum).

        Args:
            None.

        Yields:
            Tuple[str, Dict[str, int or float]] or Tuple[str, str]): desired data or "No Data" as bad result.

        """

        return self._get_statistics_by_columns_names(MedicalStaffMorbidity.isolated_columns_names)
